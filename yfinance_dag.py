from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import csv
import os
import boto3

# Default args untuk konfigurasi DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 20),  # Sesuaikan tanggal mulai DAG
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fungsi untuk mengambil data saham
def get_stock_data(tickers):
    """
    Mengambil data saham terbaru untuk daftar ticker yang diberikan.
    """
    try:
        result = []
        for ticker in tickers:
            stock = yf.Ticker(ticker)
            data = stock.history(period="1d", interval="1m")

            if data.empty:
                print(f"Tidak ada data tersedia untuk ticker '{ticker}'.")
                continue

            # Data terbaru
            last_data = data.iloc[-1]
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            stock_info = {
                "ticker": ticker,
                "datetime": current_datetime,
                "open": round(last_data["Open"], 2),
                "high": round(last_data["High"], 2),
                "low": round(last_data["Low"], 2),
                "close": round(last_data["Close"], 2)
            }
            result.append(stock_info)

        return result
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")
        return []

# Fungsi untuk mengekstrak dan transformasi data saham
def extract_transform_stock_data():
    """
    Fungsi untuk mengekstrak data saham, kemudian menyimpan ke CSV dengan nama tetap.
    """
    tickers = ["nvda", "aciw", "sony", "keys", "mstr", "dv", "oc", "rok", "mms",
               "cmcsa", "goog", "ncty", "nyt", "veon"]
    data = get_stock_data(tickers)

    # Path dan nama file tetap
    output_file = '/home/azureuser/stock/stock_data.csv'  # File selalu dengan nama yang sama

    # Simpan data ke file CSV
    if data:
        try:
            with open(output_file, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            print(f"Data berhasil disimpan ke {output_file}")
            return output_file
        except Exception as e:
            print(f"Gagal menyimpan data ke file: {e}")
            return None
    else:
        print("Tidak ada data yang berhasil diekstrak.")
        return None

def upload_to_s3(file_path, bucket_name, s3_key):
    """
    Mengunggah file ke bucket S3 menggunakan boto3.
    """
    try:
        s3_client = boto3.client("s3", aws_access_key_id="", aws_secret_access_key="")
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"File berhasil diunggah ke S3: {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Gagal mengunggah file ke S3: {e}")

# Fungsi untuk melakukan load data ke S3 dengan nama tetap
def load_stock_data_to_s3_fixed():
    """
    Mengunggah file CSV hasil ekstraksi dan transformasi ke S3 dengan nama tetap.
    """
    file_path = '/home/azureuser/stock/stock_data.csv'  # File dengan nama tetap

    if file_path and os.path.exists(file_path):
        bucket_name = ""  # Ganti dengan nama bucket S3 Anda
        s3_key = 'append_stock/stock_data.csv'  # Nama file tetap di S3
        upload_to_s3(file_path, bucket_name, s3_key)
    else:
        print("Tidak ada file yang ditemukan untuk diunggah ke S3 dengan nama tetap.")

# Fungsi untuk melakukan load data ke S3 dengan nama unik
def load_stock_data_to_s3_unique():
    """
    Mengunggah file CSV hasil ekstraksi dan transformasi ke S3 dengan nama yang mengandung timestamp.
    """
    file_path = '/home/azureuser/stock/stock_data.csv'  # File yang dihasilkan selalu dengan nama tetap
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    if file_path and os.path.exists(file_path):
        bucket_name = ""  # Ganti dengan nama bucket S3 Anda
        s3_key = f'all_stock/stock_data_{timestamp}.csv'  # Nama unik untuk S3
        upload_to_s3(file_path, bucket_name, s3_key)
    else:
        print("Tidak ada file yang ditemukan untuk diunggah ke S3 dengan nama unik.")

# Membuat DAG untuk Airflow
with DAG(
    "yahoo_etl_dag",
    default_args=default_args,
    description="ETL DAG untuk mengambil data saham dari yfinance, menyimpan ke lokal, dan mengunggah ke S3",
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:

    # Task 1: Extract dan Transformasi Data dengan nama tetap
    extract_transform_task_fixed = PythonOperator(
        task_id="extract_transform_stock_data_fixed",
        python_callable=extract_transform_stock_data,
    )

    # Task 2: Load Data ke S3 dengan nama tetap
    load_fixed_task = PythonOperator(
        task_id="load_stock_data_to_s3_fixed",
        python_callable=load_stock_data_to_s3_fixed,
    )

    # Task 3: Load Data ke S3 dengan nama unik
    load_unique_task = PythonOperator(
        task_id="load_stock_data_to_s3_unique",
        python_callable=load_stock_data_to_s3_unique,
    )

    # Alur tugas
    extract_transform_task_fixed >> load_fixed_task >> load_unique_task
