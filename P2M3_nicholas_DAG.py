import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from elasticsearch import Elasticsearch
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db

# # Function to get data from PostgreSQL
def get_data_from_db():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)  
    df.to_csv('/opt/airflow/data/P2M3_nicholas_data_raw.csv',index=False)

def data_pipeline():
    #Loading CSV to dataframe
    cleaning = pd.read_csv('/opt/airflow/data/P2M3_nicholas_data_raw.csv')

    cleaning = cleaning.drop_duplicates(keep='first')

    #melakukan lowercase pada nama kolom
    cleaning = cleaning.rename(columns=lambda x: x.lower())

    # ubah nama kolom
    cleaning = cleaning.rename(columns = {
        'customer id ': 'cust_id',
        'item purchased': 'item_purchased',
        'purchase amount (usd)': 'purchase_amount',
        'review rating': 'review_rating',
        'subscription status': 'subsciption_status',
        'payment method': 'payment_method',
        'shipping type': 'shipping_type',
        'discount applied': 'discount_applied',
        'promo code used': 'promo_code',
        'previous purchases': 'previous_purchases',
        'preferred payment method': 'preferred_payment_method',
        'frequency of purchases': 'freq_of_purchases'
    })
    
    #mengubah tipe data yang keliru
    cleaning["purchase_amount"] = cleaning["purchase_amount"].astype(float)
    
    #menghapus data yang mengandung missing value
    cleaning.dropna(inplace=True)

    cleaning.to_csv('/opt/airflow/data/P2M3_nicholas_clean_data.csv', index=False)

    


# Function to post the data to Kibana
def post_to_kibana():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/data/P2M3_nicholas_clean_data.csv')
    
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="table_m3", id=i+1, body=doc)
        # print(res)


# DAG setup
default_args = {
    'owner': 'nicholas',
    'depends_on_past': False,
    'email_on_failure': False, #Parameter ini mengontrol apakah notifikasi email akan dikirim jika task mengalami kegagalan.
    'email_on_retry': False, #Parameter ini mengontrol apakah notifikasi email akan dikirim jika task dijadwalkan ulang (retry).
    'retries': 1, #menentukan berapa kali task akan mencoba dijalankan ulang jika terjadi kegagalan.
    'retry_delay': timedelta(minutes=60), #menentukan berapa lama (dalam satuan waktu) Apache Airflow harus menunggu sebelum mencoba menjalankan ulang task jika terjadi kegagalan. Dalam kasus ini, task akan dijadwalkan ulang setiap 60 menit (1 jam) jika diperlukan
    #
}

with DAG('nicholas_m3',
         description='tugas milestone',
         default_args=default_args,
         schedule_interval='@daily', # mengatur frekuensi eksekusi DAG. Dalam hal ini, DAG ini dijadwalkan untuk berjalan setiap hari
         start_date=datetime(2023, 10, 27), #menunjukkan tanggal dan waktu saat DAG akan mulai dijalankan. 27 Oktober 2023
         catchup=False) as dag: #Airflow tidak akan mengejar eksekusi yang tertinggal sebelum tanggal start_date. 
        #Jika ada pekerjaan yang seharusnya dijalankan di hari-hari sebelum tanggal mulai, itu tidak akan dieksekusi secara otomatis
    
    # Task to fetch data from PostgreSQL
    fetch_task = PythonOperator(
        task_id='get_data_from_db',
        python_callable=get_data_from_db
    )
    
    # Task yg akan di eksekusi pythonoperator
    clean_task = PythonOperator(
        task_id='cleaning_data',
        python_callable=data_pipeline
    )
    
     # Task to post to Kibana
    post_to_kibana_task = PythonOperator(
        task_id='post_to_kibana',
        python_callable=post_to_kibana
    )
    
    #merancang urutan airflow
    fetch_task >> clean_task >> post_to_kibana_task