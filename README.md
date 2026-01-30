# 🌱 Sustainability Analytics SaaS (GCP + BigQuery)

A cloud-native ESG and sustainability analytics platform for e-commerce and businesses.

This system allows companies to:

- Upload sales & operations data (CSV)
- Upload utility/invoice data (electricity, fuel, courier, etc.)
- Automatically calculate CO₂ emissions
- View monthly trends and company footprint
- Maintain audit logs
- Reset and manage datasets
- Visualize insights using dashboards

Built on Google Cloud Platform.

---

## 📌 Features

✅ Product-based carbon estimation  
✅ Utility bill-based carbon estimation  
✅ Monthly CO₂ trends  
✅ CO₂ per unit efficiency  
✅ Company-wide footprint  
✅ Upload history & soft delete  
✅ Reset system  
✅ Interactive dashboard  
✅ BigQuery analytics layer  
✅ Cloud Run backend  

---

## 🏗 Architecture

# 🌱 Sustainability Analytics SaaS (GCP + BigQuery)

A cloud-native ESG and sustainability analytics platform for e-commerce and businesses.

This system allows companies to:

- Upload sales & operations data (CSV)
- Upload utility/invoice data (electricity, fuel, courier, etc.)
- Automatically calculate CO₂ emissions
- View monthly trends and company footprint
- Maintain audit logs
- Reset and manage datasets
- Visualize insights using dashboards

Built on Google Cloud Platform.

---

## 📌 Features

✅ Product-based carbon estimation  
✅ Utility bill-based carbon estimation  
✅ Monthly CO₂ trends  
✅ CO₂ per unit efficiency  
✅ Company-wide footprint  
✅ Upload history & soft delete  
✅ Reset system  
✅ Interactive dashboard  
✅ BigQuery analytics layer  
✅ Cloud Run backend  

---

## 🏗 Architecture
Frontend (HTML + JS + Chart.js)
|
v
FastAPI Backend (Cloud Run)
|
v
BigQuery + Cloud Storage

---

---

## 📦 Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | FastAPI (Python) |
| Database | BigQuery |
| Storage | Google Cloud Storage |
| Hosting | Cloud Run |
| Charts | Chart.js |
| Auth | GCP IAM |
| Build | Cloud Build |

---

## 📋 Prerequisites

Before starting, install:

### 1️⃣ Google Cloud SDK

```bash
https://cloud.google.com/sdk/docs/install

Verify: 

gcloud --version
bq --version

python3 --version

docker --version

####Project Setup
###Step 1 — Login & Set Project

gcloud auth login
gcloud auth application-default login

gcloud config set project YOUR_PROJECT_ID

####Step 2 — Enable Required APIs

gcloud services enable \
  bigquery.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  storage.googleapis.com

###Step 3 — Create Storage Bucket

gsutil mb -l asia-south1 gs://YOUR_BUCKET_NAME

###Step 4 — Create BigQuery Dataset

bq mk --location=asia-south1 sustainability_ds

###Step 5 — Create Tables

bq mk \
--table sustainability_ds.operations \
product_id:STRING,units_sold:INTEGER,energy_kwh:FLOAT,transport_km:FLOAT,record_date:DATE

###PRODUCT CATALOGUE

bq mk \
--table sustainability_ds.product_catalogue_table \
product_id:STRING,product_name:STRING,category:STRING

###UTILITY BILLS

bq mk \
--table sustainability_ds.utility_bills \
month:DATE,region:STRING,bill_type:STRING,units:FLOAT

###UPLOAD LOG

bq mk \
--table sustainability_ds.upload_log \
upload_id:STRING,upload_time:TIMESTAMP,file_name:STRING,rows_loaded:INTEGER,status:STRING

###CREATE ANALYTICS VIEWS
###MONTHLY METRICS
bq query --use_legacy_sql=false "
CREATE OR REPLACE VIEW sustainability_ds.monthly_metrics AS
SELECT
  FORMAT_DATE('%Y-%m', o.record_date) AS month,
  TRIM(UPPER(o.product_id)) AS product_id,
  p.product_name,
  p.category,
  SUM(o.units_sold) AS total_units,
  SUM(o.energy_kwh)*0.82 AS energy_co2,
  SUM(o.transport_km)*0.0525 AS transport_co2,
  (SUM(o.energy_kwh)*0.82 + SUM(o.transport_km)*0.0525) AS total_co2
FROM sustainability_ds.operations o
JOIN sustainability_ds.product_catalogue_table p
ON TRIM(UPPER(o.product_id))=TRIM(UPPER(p.product_id))
GROUP BY month, product_id, p.product_name, p.category;
"

###BILL EMISSIONS

bq query --use_legacy_sql=false "
CREATE OR REPLACE VIEW sustainability_ds.bill_emissions AS
SELECT
  month,
  region,
  bill_type,
  SUM(
    CASE
      WHEN bill_type='electricity' THEN units*0.7
      WHEN bill_type='fuel' THEN units*2.6
      WHEN bill_type='courier' THEN units*0.05
      ELSE 0
    END
  ) AS estimated_co2
FROM sustainability_ds.utility_bills
GROUP BY month,region,bill_type;
"

###COMPANY EMISSIONS

bq query --use_legacy_sql=false "
CREATE OR REPLACE VIEW sustainability_ds.company_emissions AS

SELECT
  month,
  'product' AS source,
  SUM(total_co2) AS co2
FROM sustainability_ds.monthly_metrics
GROUP BY month

UNION ALL

SELECT
  FORMAT_DATE('%Y-%m', month),
  'utility',
  SUM(estimated_co2)
FROM sustainability_ds.bill_emissions
GROUP BY month;
"
###BACKEND SETUP

###Step 1 — Install Dependencies

fastapi
uvicorn
google-cloud-bigquery
google-cloud-storage
python-multipart

###Install
pip install -r requirements.txt

###Step 2 - Local Test

uvicorn main:app --reload --port 8080

###Visit:
http://localhost:8080

###DEPLOY TO CLOUD RUN
###TEP 1 - BUILD

glcoud builds submit

###Step 2 - Deploy

gcloud run deploy sustainability-backend \
  --region asia-south1 \
  --allow-unauthenticated \
  --set-env-vars BUCKET_NAME=YOUR_BUCKET_NAME

###CSV Format

product_id,units_sold,energy_kwh,transport_km,record_date
P1,100,40,120,2026-01-10
P2,80,35,100,2026-01-15

###Utility Bill Format

month,region,bill_type,units
2026-01-01,India,electricity,900
2026-01-01,India,fuel,300

Production Notes
Emission factors are configurable
Views auto-refresh
Supports multi-region data
Designed for SaaS deployment
Compatible with GCP Marketplace

Roadmap (V2 / V3)
ESG scoring
Compliance reports (PDF)
Supplier tracking
AI forecasting
Scope-3 emissions
API integrations
ERP sync