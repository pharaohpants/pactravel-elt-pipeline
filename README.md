
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
Data berasal dari domain PacTravel dengan entitas:
1. aircrafts
2. airlines
3. airports
4. customers
5. hotel
6. flight_bookings
7. hotel_bookings

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

### Select Business Process
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
Urutan workflow:
1. Extract data dari source DB.
2. Load data ke schema staging pada DWH.
3. Transform staging menjadi dimension dan fact di schema final.
4. Jalankan dbt test untuk validasi kualitas data.

### Scheduling and Alerting
1. Scheduler harian menggunakan Python schedule.
2. Logging menggunakan loguru.
3. Alerting/error capture menggunakan sentry-sdk.

### Execution Commands
[BUKTI WAJIB]
1. Perintah extract-load:
	- .\.venv\Scripts\python.exe -m luigi --module pipeline.extract_load RunAllExtractLoad --local-scheduler
2. Perintah transform dan test:
	- Set-Location .\pactravel_dwh
	- ..\.venv\Scripts\dbt.exe parse
	- ..\.venv\Scripts\dbt.exe build --full-refresh
3. Lampirkan screenshot log sukses untuk Luigi dan dbt.

## Step 4 - Show Results of the Pipeline

### A. Proof that ELT Runs Successfully
[BUKTI WAJIB]
1. Lampirkan output terminal sukses Luigi run.
2. Lampirkan output terminal sukses dbt run/build.
3. Lampirkan output terminal sukses dbt test.

### B. Proof Data Loaded into Final Tables
Contoh query:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'final'
ORDER BY table_name;

SELECT COUNT(*) AS rows_fct_flight_bookings FROM final.fct_flight_bookings;
SELECT COUNT(*) AS rows_fct_hotel_bookings  FROM final.fct_hotel_bookings;
SELECT COUNT(*) AS rows_daily_flights       FROM final.fct_daily_flights_summary;
SELECT COUNT(*) AS rows_daily_hotels        FROM final.fct_daily_hotels_summary;
```

[BUKTI WAJIB]
1. Tempel output query jumlah baris setiap tabel.

### C. Proof Business Requirement Coverage
Daily booking volume:

```sql
SELECT full_date, total_bookings
FROM final.fct_daily_flights_summary
ORDER BY full_date
LIMIT 30;

SELECT full_date, total_bookings
FROM final.fct_daily_hotels_summary
ORDER BY full_date
LIMIT 30;
```

Average ticket price over time:

```sql
SELECT full_date, avg_ticket_price
FROM final.fct_daily_flights_summary
ORDER BY full_date
LIMIT 30;
```

[BUKTI WAJIB]
1. Tempel output query daily volume flights.
2. Tempel output query daily volume hotels.
3. Tempel output query average ticket price over time.

### D. Data Quality and Sanity Checks
Contoh query sanity:

```sql
SELECT COUNT(*) AS null_flight_date_key
FROM final.fct_flight_bookings
WHERE date_key IS NULL;

SELECT COUNT(*) AS null_hotel_check_in_date_key
FROM final.fct_hotel_bookings
WHERE check_in_date_key IS NULL;

SELECT COUNT(*) AS null_hotel_check_out_date_key
FROM final.fct_hotel_bookings
WHERE check_out_date_key IS NULL;
```

[BUKTI WAJIB]
1. Tempel output query sanity check.
2. Jelaskan jika ada nilai null dan tindakan perbaikannya.



