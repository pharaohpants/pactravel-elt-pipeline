
# Pactravel ELT and Data Warehouse Report

## Project Objective
Project ini bertujuan untuk:
1. Membangun skema Data Warehouse untuk domain travel.
2. Membangun pipeline ELT dengan Python, Luigi, dan dbt.
3. Menyediakan data siap analisis untuk kebutuhan bisnis utama:
	- Daily booking volume flights dan hotels.
	- Average ticket price over time.

## Step 1 - Requirements Gathering

### Data Source
Data berasal dari : https://github.com/Kurikulum-Sekolah-Pacmann/pactravel-dataset.git 

dengan ERD : 
<img width="973" height="743" alt="image" src="https://github.com/user-attachments/assets/20a66c2b-841d-4044-8122-bd8c533bee54" />


### Description
Data mencerminkan aktivitas perjalanan customer pada pemesanan tiket pesawat dan hotel. Setiap transaksi booking memiliki atribut waktu, customer, dan atribut operasional lain yang relevan untuk analisis tren.

### Problem
Stakeholder membutuhkan:
1. Monitoring volume booking harian untuk flight dan hotel.
2. Monitoring average ticket price dari waktu ke waktu.
3. Dataset yang konsisten untuk analisis perilaku customer.

### Solution
Solusi yang diimplementasikan:
1. Menyusun model dimensional pada schema final.
2. Menjalankan EL (extract-load) ke schema staging dengan Luigi.
3. Menjalankan transformasi dan data quality test dengan dbt.
4. Menyediakan fact table transaksi dan fact table agregasi harian.

## Step 2 - Designing Data Warehouse Model

### Business Process
Business process utama:
1. Flight booking transaction.
2. Hotel booking transaction.
3. Daily performance summary untuk flight dan hotel.

### Declare Grain
Grain yang digunakan:
1. fct_flight_bookings: satu baris merepresentasikan satu kombinasi unik trip_id + flight_number + seat_number.
2. fct_hotel_bookings: satu baris merepresentasikan satu hotel booking (trip_id).
3. fct_daily_flights_summary: satu baris per tanggal.
4. fct_daily_hotels_summary: satu baris per tanggal.

### Dimensions
Dimension yang digunakan:
1. dim_date
2. dim_customers
3. dim_aircrafts
4. dim_airlines
5. dim_airports
6. dim_hotel

### Facts
Fact yang digunakan:
1. Transaction fact:
	- fct_flight_bookings
	- fct_hotel_bookings
2. Periodic snapshot / aggregated daily fact:
	- fct_daily_flights_summary
	- fct_daily_hotels_summary

### Slowly Changing Dimension Strategy
Strategi SCD pada dim_customers:
1. Type 2 dengan kolom effective_start, effective_end, is_current.
2. Perubahan atribut customer menghasilkan versi row baru.
3. Versi lama ditutup dengan is_current = false.

### ERD and Model Diagram
[BUKTI WAJIB]
1. Lampirkan ERD dimensional model.
2. Lampirkan relasi antar dimension dan fact.
3. Lampirkan screenshot struktur schema final di database.

## Step 3 - Data Pipeline Implementation

### Architecture
Komponen pipeline:
1. Source database (Postgres).
2. DWH database (Postgres) dengan schema staging dan final.
3. Luigi untuk extract-load.
4. dbt untuk transformasi dan testing.
5. Scheduler dan alerting menggunakan schedule, loguru, dan sentry-sdk.

### Workflow 

<img width="2848" height="2621" alt="image" src="https://github.com/user-attachments/assets/8ae5ac3d-db14-4172-b6a3-f7887697e227" />

Urutan workflow:
1. Extract data dari source DB.
2. Load data ke schema staging pada DWH.
3. Transform staging menjadi dimension dan fact di schema final.
4. Jalankan dbt test untuk validasi kualitas data.

### Execution Commands
1. Perintah extract-load:
	- .\.venv\Scripts\python.exe -m luigi --module pipeline.extract_load RunAllExtractLoad --local-scheduler
2. Perintah transform dan test:
	- Set-Location .\pactravel_dwh
	- ..\.venv\Scripts\dbt.exe parse
	- ..\.venv\Scripts\dbt.exe build --full-refresh
3. Lampirkan screenshot log sukses untuk Luigi dan dbt.

## Step 4 Results of the Pipeline

### A. ELT Runs Successfully
<img width="842" height="572" alt="image" src="https://github.com/user-attachments/assets/22ac23bb-3a50-4cdf-a3b4-192bab5c5dfd" />
<img width="862" height="627" alt="image" src="https://github.com/user-attachments/assets/c4082589-d403-4db1-8104-ee6cf4dc9f77" />


### B. Data Loaded into Final Tables
<img width="417" height="320" alt="image" src="https://github.com/user-attachments/assets/a892f41d-f5df-431c-9efb-f1ebbee4d8cc" />


### C. Business Requirement Coverage
Daily booking volume:

<img width="398" height="673" alt="image" src="https://github.com/user-attachments/assets/f604b5c1-37e8-4de5-a378-03cd52ac1ba5" />


Average ticket price over time:
<img width="362" height="667" alt="image" src="https://github.com/user-attachments/assets/5e203944-2657-4dfc-ab61-6b7c0615643a" />


### D. Data Quality and Sanity Checks
<img width="442" height="496" alt="image" src="https://github.com/user-attachments/assets/23e6da55-794a-4f74-8973-abeb79eea56f" />




