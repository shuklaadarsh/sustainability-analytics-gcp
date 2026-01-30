from fastapi import FastAPI, UploadFile, File, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google.cloud import storage, bigquery
from typing import Optional
import uuid
import os

app = FastAPI()

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
DATASET = "sustainability_ds"
TABLE = "operations"

# Mount static safely
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def home():
    return FileResponse("static/index.html")

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):

    filename = f"{uuid.uuid4()}_{file.filename}"

    try:
        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_file(file.file)

        bq_client = bigquery.Client()

        table_id = f"{DATASET}.{TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",   # IMPORTANT
            schema=[
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("units_sold", "INTEGER"),
                bigquery.SchemaField("energy_kwh", "FLOAT"),
                bigquery.SchemaField("transport_km", "FLOAT"),
                bigquery.SchemaField("record_date", "DATE"),
            ],
            allow_quoted_newlines=True,
            ignore_unknown_values=True
        )

        uri = f"gs://{BUCKET_NAME}/{filename}"

        load_job = bq_client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )

        load_job.result()

        rows_loaded = load_job.output_rows

        # Log upload
        bq_client.query(f"""
        INSERT INTO sustainability_ds.upload_log
        (upload_id, upload_time, file_name, rows_loaded, status)
        VALUES
        ('{filename}', CURRENT_TIMESTAMP(), '{file.filename}', {rows_loaded}, 'SUCCESS')
        """).result()

        return {"message": "Upload successful", "rows": rows_loaded}

    except Exception as e:

        return {
            "error": "Upload failed",
            "details": str(e)
        }

@app.post("/upload-bill")
async def upload_bill(
    bill_type: str,
    amount: float,
    units: float,
    region: str,
    month: str
):
    bq_client = bigquery.Client()

    bill_id = str(uuid.uuid4())

    query = f"""
    INSERT INTO sustainability_ds.utility_bills
    (bill_id, bill_type, amount, units, region, month, upload_time)
    VALUES
    (
      '{bill_id}',
      '{bill_type}',
      {amount},
      {units},
      '{region}',
      DATE('{month}-01'),
      CURRENT_TIMESTAMP()
    )
    """

    bq_client.query(query).result()

    return {
        "status": "success",
        "bill_id": bill_id
    }

@app.get("/metrics")
def get_metrics(since: Optional[str] = Query(None)):
    bq_client = bigquery.Client()

    where_clause = ""
    if since:
        where_clause = f"WHERE record_date >= DATE('{since}')"

    query = f"""
    SELECT
      o.product_id,
      p.product_name,
      p.category,
      SUM(o.units_sold) AS total_units_sold,
      SUM(o.energy_kwh) AS total_energy_kwh,
      SUM(o.transport_km) AS total_transport_km
    FROM sustainability_ds.operations o
    LEFT JOIN sustainability_ds.product_catalogue_table p
      ON o.product_id = p.product_id
    {where_clause}
    GROUP BY o.product_id, p.product_name, p.category
    ORDER BY o.product_id
    """

    results = bq_client.query(query).result()

    data = []
    for row in results:
        data.append({
            "product_id": row.product_id,
            "product_name": row.product_name or row.product_id,
            "category": row.category or "Unknown",
            "total_units_sold": row.total_units_sold,
            "energy_co2_kg": row.total_energy_kwh * 0.82,
            "transport_co2_kg": row.total_transport_km * 0.0525,
            "total_co2_kg": (
                row.total_energy_kwh * 0.82 +
                row.total_transport_km * 0.0525
            ),
        })

    return {
        "count": len(data),
        "data": data
    }


@app.get("/uploads")
def get_upload_history():
    bq_client = bigquery.Client()

    query = """
    SELECT
      upload_id,
      upload_time,
      file_name,
      rows_loaded,
      status
    FROM sustainability_ds.upload_log
    WHERE status != 'DELETED'
    ORDER BY upload_time DESC
    LIMIT 20
    """

    results = bq_client.query(query).result()

    data = []
    for row in results:
        data.append({
            "upload_id": row.upload_id,
            "upload_time": str(row.upload_time),
            "file_name": row.file_name,
            "rows_loaded": row.rows_loaded,
            "status": row.status
        })

    return {
        "count": len(data),
        "data": data
    }


@app.delete("/uploads/{upload_id}")
def delete_upload(upload_id: str):
    bq_client = bigquery.Client()

    bq_client.query(f"""
    UPDATE sustainability_ds.upload_log
    SET status = 'DELETED'
    WHERE upload_id = '{upload_id}'
    """).result()

    return {"status": "deleted", "upload_id": upload_id}

@app.get("/trends")
def get_trends():

    bq_client = bigquery.Client()

    query = """
    SELECT
      FORMAT_DATE('%Y-%m', record_date) AS month,

      SUM(
        (energy_kwh * 0.82) +
        (transport_km * 0.0525)
      ) AS total_co2,

      SUM(units_sold) AS total_units

    FROM sustainability_ds.operations

    GROUP BY month
    ORDER BY month
    """

    results = bq_client.query(query).result()

    data = []

    for row in results:

        total = float(row.total_co2 or 0)
        units = float(row.total_units or 0)

        cpu = total / units if units > 0 else 0

        data.append({
            "month": row.month,              # ALWAYS STRING YYYY-MM
            "total_co2": round(total, 2),
            "co2_per_unit": round(cpu, 4)
        })

    return {"data": data}


@app.get("/bill-insights")
def get_bill_insights():

    bq_client = bigquery.Client()

    query = """
    SELECT
      FORMAT_DATE('%Y-%m', month) AS month,
      region,
      bill_type,
      estimated_co2
    FROM sustainability_ds.bill_emissions
    ORDER BY month
    """

    results = bq_client.query(query).result()

    data = []

    for row in results:
        data.append({
            "month": row.month,
            "region": row.region,
            "bill_type": row.bill_type,
            "estimated_co2": float(row.estimated_co2)
        })

    return {"data": data}

@app.get("/company-kpis")
def get_company_kpis():

    bq_client = bigquery.Client()

    query = """
    SELECT
      SUM(co2) AS total_co2
    FROM sustainability_ds.company_emissions
    """

    result = list(bq_client.query(query).result())

    total = float(result[0].total_co2) if result and result[0].total_co2 else 0

    return {
        "total_company_co2": total
    }

@app.get("/total-footprint")
def get_total_footprint():

    bq_client = bigquery.Client()

    query = """
    SELECT
      month,

      SUM(co2) AS total_co2,

      SUM(CASE WHEN source='product' THEN co2 ELSE 0 END) AS product_co2,
      SUM(CASE WHEN source='utility' THEN co2 ELSE 0 END) AS utility_co2

    FROM sustainability_ds.company_emissions
    GROUP BY month
    ORDER BY month
    """

    results = bq_client.query(query).result()

    data = []

    for row in results:
        data.append({
            "month": row.month,
            "total_co2": float(row.total_co2),
            "product_co2": float(row.product_co2),
            "utility_co2": float(row.utility_co2)
        })

    return {"data": data}

@app.delete("/reset-all")
def reset_all_data():

    bq_client = bigquery.Client()

    # Delete CSV operations
    bq_client.query("""
    DELETE FROM sustainability_ds.operations
    WHERE TRUE
    """).result()

    # Delete utility bills
    bq_client.query("""
    DELETE FROM sustainability_ds.utility_bills
    WHERE TRUE
    """).result()

    # Delete upload history
    bq_client.query("""
    DELETE FROM sustainability_ds.upload_log
    WHERE TRUE
    """).result()

    return {"status": "all_data_cleared"}
