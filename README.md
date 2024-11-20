# Workflow ETL dengan Apache Airflow: MarketAux dan Yahoo Finance

Repository ini berisi dua workflow ETL yang diimplementasikan menggunakan **Apache Airflow** dan dijalankan pada **Virtual Machine (VM) Azure**. Workflow ini mengotomatisasi proses ekstraksi, transformasi, dan pemuatan (ETL) data dari dua sumber: **MarketAux API** dan **Yahoo Finance**. Data yang telah diproses akan disimpan secara lokal dan diunggah ke bucket **S3**.

> [!NOTE]
> API dan kredensial lainnya dalam source code ini telah disembunyikan untuk menjaga privasi dan keamanan.  
> Silakan gunakan kredensial Anda sendiri jika ingin menjalankan project ini.

## Kelompok W
- Anggota 1: Muhammad Fajrulfalaq Izzulfirdausyah Suryaprabandaru - 22/494174/TK/54184
- Anggota 2: Reihan Athar Zildaniar - 22/497191/TK/54482
- Anggota 3: Syaifullah Hilmi Ma'arij - 22/497775/TK/54568

---

## **Link**
- **Blog Post**: [Notion](#)
- **Video Presentasi**: [Youtube](#)

## **Reference API**
- [MarketAux](https://www.marketaux.com/)
- [Yahoo Finance](https://pypi.org/project/yfinance/)

---

## **Deskripsi Workflow**

### 1. **Workflow MarketAux**
- **Tujuan**: Mengambil data berita spesifik industri menggunakan MarketAux API.
- **Tugas**:
  - Mengambil data berita berdasarkan industri tertentu.
  - Menyimpan data ke direktori lokal:
    - `all_csv/` (dengan nama file unik berisi timestamp).
    - `append_csv/` (dengan nama file tetap).
  - Mengunggah file ke folder yang sesuai di bucket S3.

### 2. **Workflow Yahoo Finance**
- **Tujuan**: Mengambil data pasar saham untuk daftar ticker tertentu.
- **Tugas**:
  - Mengambil data real-time saham dari Yahoo Finance.
  - Menyimpan data secara lokal di:
    - `stock/stock_data.csv` (dengan nama file tetap).
  - Mengunggah data ke bucket S3 dengan:
    - Nama tetap (`append_stock/stock_data.csv`).
    - Nama unik yang mengandung timestamp (`all_stock/stock_data_<timestamp>.csv`).

---

## **Arsitektur Teknologi**
![Arsitektur Workflow ETL](https://github.com/saaip7/marketaux_yfinance_etl_airflow/blob/main/architec.jpeg)

- **Apache Airflow**: Mengorkestrasi proses ETL untuk kedua workflow.
- **Azure VM**: Menyediakan lingkungan eksekusi untuk Airflow.
- **Sumber Data**:
  - **MarketAux API**: Menyediakan data berita berdasarkan industri.
  - **Yahoo Finance**: Menyediakan data pasar saham secara real-time.
- **S3 Bucket**: Menyimpan data yang telah diproses dengan struktur folder terorganisasi.
- **Red Shift**: Menyimpan data realtime dari S3 Bucket ke dalam database table.
- **Power BI**: Melakukan visualisasi data dari Red Shift.


### **Memulai Airflow**
1. SSH ke Azure VM:
2. Jalankan scheduler dan webserver Airflow:
   ```bash
   airflow scheduler
   airflow webserver -p 8080
   ```
3. Akses UI Airflow di port `http://<ip-vm-azure-anda>:8080`. Atau bisa juga di `http://localhost:8080`

### **Menjalankan DAG**
1. **Workflow MarketAux**:
   - Nama DAG: `marketaux_etl_dag`
   - Jadwal: Setiap 20 menit (`*/20 * * * *`).
2. **Workflow Yahoo Finance**:
   - Nama DAG: `yahoo_etl_dag`
   - Jadwal: Setiap 5 menit (`*/5 * * * *`).

---

## **Output**

### **Workflow MarketAux**
- **File Lokal**:
  - `all_csv/marketaux_data_<timestamp>.csv`
  - `append_csv/marketaux_data.csv`
- **Struktur S3**:
  - `all_csv/marketaux_data_<timestamp>.csv`
  - `append_csv/marketaux_data.csv`

### **Workflow Yahoo Finance**
- **File Lokal**:
  - `stock/stock_data.csv`
- **Struktur S3**:
  - `all_stock/stock_data_<timestamp>.csv`
  - `append_stock/stock_data.csv`
