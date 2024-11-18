'''
=================================================
Milestone 3 - DAG 

Nama  : Muhammad Aliansyah Ramadhan
Batch : RMT-035

Program ini dibuat untuk melakukan automatisasi transformasi dan load data dari PostgreSQL ke Elasticsearch.
Adapun dataset yang dipakai adalah dataset mengenai informasi pegawai, termasuk nama, jenis kelamin, departemen, gaji, dan data lainnya terkait kehadiran serta lembur pegawai.
=================================================
'''

import os
import pandas as pd
import psycopg2 as db
import datetime as dt
from datetime import timedelta
from io import StringIO
from pytz import timezone

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Path untuk file CSV
csv_file_path = '/opt/airflow/dags/P2M3_aliansyahr_data_raw.csv'
cleaned_csv_path = '/opt/airflow/dags/P2M3_aliansyahr_data_clean.csv'

def save_to_sql():
    """
    Fungsi ini membaca data dari file CSV dan menyimpannya ke PostgreSQL dengan 
    menggunakan metode upsert untuk mencegah duplikasi data.

    """
    try:
        # Koneksi ke PostgreSQL
        conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
        conn = db.connect(conn_string)
        cur = conn.cursor()

        # Membuat tabel jika belum ada
        sql = '''
            CREATE TABLE IF NOT EXISTS table_m3 (
                "No" int PRIMARY KEY,
                "first_name" VARCHAR(255),
                "last_name" VARCHAR(255),
                "gender" VARCHAR(255),
                "start_date" VARCHAR(255),
                "years" INT,
                "department" VARCHAR(255),
                "country" VARCHAR(255),
                "center" VARCHAR(255),
                "monthly_salary" FLOAT,
                "annual_salary" FLOAT,
                "job_rate" VARCHAR(255),
                "sick_leaves" INT,
                "unpaid_leaves" INT,
                "overtime_hours" INT
            );
        '''
        cur.execute(sql)
        conn.commit()

        # Memeriksa apakah file CSV ada
        if not os.path.exists(csv_file_path):
            print(f"Error: File tidak ditemukan - {csv_file_path}")
            return
        
        # Membaca file CSV
        df = pd.read_csv(csv_file_path)

        # Memeriksa struktur DataFrame
        print("DataFrame sebelum dimasukkan ke SQL:")
        print(df.head())
        print(df.dtypes)

        # Memasukkan data ke PostgreSQL dengan upsert untuk menghindari duplikasi
        for index, row in df.iterrows():
            insert_sql = '''
                INSERT INTO table_m3 ("No", "first_name", "last_name", "gender", "start_date", "years", "department", "country", 
                                      "center", "monthly_salary", "annual_salary", "job_rate", "sick_leaves", "unpaid_leaves", "overtime_hours")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("No") 
                DO UPDATE SET 
                    "first_name" = EXCLUDED."first_name",
                    "last_name" = EXCLUDED."last_name",
                    "gender" = EXCLUDED."gender",
                    "start_date" = EXCLUDED."start_date",
                    "years" = EXCLUDED."years",
                    "department" = EXCLUDED."department",
                    "country" = EXCLUDED."country",
                    "center" = EXCLUDED."center",
                    "monthly_salary" = EXCLUDED."monthly_salary",
                    "annual_salary" = EXCLUDED."annual_salary",
                    "job_rate" = EXCLUDED."job_rate",
                    "sick_leaves" = EXCLUDED."sick_leaves",
                    "unpaid_leaves" = EXCLUDED."unpaid_leaves",
                    "overtime_hours" = EXCLUDED."overtime_hours";
            '''
            cur.execute(insert_sql, tuple(row))
        
        conn.commit()
        print("Data berhasil dimasukkan ke PostgreSQL.")
    except Exception as e:
        print(f"Error pada save_to_sql: {e}")
    finally:
        # Memastikan cursor dan koneksi ditutup
        if cur:
            cur.close()
        if conn:
            conn.close()


def fetch_and_clean_data():
    """
    Fungsi ini mengambil data dari PostgreSQL, melakukan pembersihan data 
    seperti menghilangkan duplikasi, menangani missing value, dan menyimpan 
    data bersih ke file CSV.

    """
    try:
        # Koneksi ke PostgreSQL
        conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
        conn = db.connect(conn_string)
        query = "SELECT * FROM table_m3;"

        # Mengambil data dari PostgreSQL
        df = pd.read_sql(query, conn)
        conn.close()

        # Memeriksa apakah DataFrame kosong
        if df.empty:
            print("Tidak ada data ditemukan di table_m3.")
            return  # Keluar dari fungsi jika tidak ada data yang diambil

        # Data Cleaning
        # Mengganti koma dengan titik di kolom 'job_rate' dan mengkonversi ke float
        if 'job_rate' in df.columns:
            df['job_rate'] = df['job_rate'].str.replace(',', '.').astype(float)

        # Mengkonversi 'start_date' ke tipe datetime
        if 'start_date' in df.columns: 
            df['start_date'] = pd.to_datetime(df['start_date'], format='%d/%m/%Y', errors='coerce')
        
        # Menghapus duplikasi
        df = df.drop_duplicates()

        # Normalisasi nama kolom
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace(r'\W', '', regex=True)

        # Menangani missing value
        for column in df.columns:
            if df[column].dtype in ['float64', 'int64']:
                df[column].fillna(df[column].median(), inplace=True)
            else:
                df[column].fillna('Unknown', inplace=True)

        # Menyimpan data yang telah dibersihkan ke file CSV
        df.to_csv(cleaned_csv_path, index=False)
        print("Data bersih berhasil disimpan.")

    except Exception as e:
        print(f"Error pada fetch_and_clean_data: {e}")
    finally:
        if conn:
            conn.close()


def send_to_elastic():
    """
    Fungsi ini mengirimkan data yang telah dibersihkan dari file CSV ke Elasticsearch, 
    dengan menggunakan ID unique ('No') untuk mencegah duplikasi.

    """
    try:
        # Memeriksa apakah file CSV yang sudah dibersihkan ada
        if not os.path.exists(cleaned_csv_path):
            print(f"Error: File tidak ditemukan - {cleaned_csv_path}")
            return

        # Koneksi ke Elasticsearch
        es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
        df = pd.read_csv(cleaned_csv_path)

        # Membuat daftar aksi untuk bulk indexing dengan ID unique (No)
        actions = [
            {
                "_index": "milestone",
                "_id": r['No'],  # Menggunakan 'No' sebagai ID unique untuk mencegah duplikasi
                "_source": r.to_dict()
            }
            for _, r in df.iterrows()
        ]

        # Melakukan bulk indexing
        bulk(es, actions)
        print(f"Berhasil mengindeks {len(actions)} dokumen ke Elasticsearch.")
    except Exception as e:
        print(f"Error pada send_to_elastic: {e}")


# Konfigurasi DAG
default_args = {
    'owner': 'ali',
    'start_date': dt.datetime(2024, 10, 13, tzinfo=timezone('Asia/Jakarta')),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG('P2M3_aliansyahr_DAG', 
         default_args=default_args,
         schedule_interval='30 6 * * *',
         catchup=False
         ) as dag:

    print_starting = BashOperator(task_id='starting',
                                  bash_command='echo "Saya sedang membaca CSV sekarang....."')

    csvSQL = PythonOperator(task_id='csv_to_SQL',
                             python_callable=save_to_sql)
    
    fetch_clean = PythonOperator(task_id='fetch_n_clean', 
                                 python_callable=fetch_and_clean_data)
    
    from_csv_to_elastic = PythonOperator(task_id='to_elastic_fr_csv',
                                         python_callable=send_to_elastic)
# Alur kerja yang akan di lakukan airflow. 
print_starting >> csvSQL >> fetch_clean >> from_csv_to_elastic