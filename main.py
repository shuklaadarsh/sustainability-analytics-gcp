from fastapi import FastAPI, UploadFile, File, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google.cloud import storage, bigquery
from typing import Optional
import io
import pandas as pd
from fastapi.responses import StreamingResponse
import uuid
import os
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.colors import grey, black, lightgrey
from reportlab.lib.units import inch
from datetime import datetime
import base64


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
    month: str   # Expecting YYYY-MM
):

    bq_client = bigquery.Client()

    bill_id = str(uuid.uuid4())

    # Validate month format (YYYY-MM)
    if not month or len(month) != 7 or month[4] != "-":
        return {"error": "Invalid month format. Use YYYY-MM"}


    # Emission factors
    FACTORS = {
        "electricity": 0.82,
        "fuel": 2.68,
        "courier": 0.18
    }

    factor = FACTORS.get(bill_type, 0.0)
    estimated_co2 = units * factor


    # SAFE month parsing
    bq_client.query(f"""
    INSERT INTO sustainability_ds.utility_bills
    (
      bill_id,
      bill_type,
      amount,
      units,
      region,
      month,
      upload_time
    )
    VALUES
    (
      '{bill_id}',
      '{bill_type}',
      {amount},
      {units},
      '{region}',
      PARSE_DATE('%Y-%m-%d', '{month}-01'),
      CURRENT_TIMESTAMP()
    )
    """).result()


    return {
        "status": "success",
        "bill_id": bill_id,
        "estimated_co2": estimated_co2
    }


@app.get("/metrics")
def get_metrics(since: Optional[str] = Query(None)):

    region = os.environ.get("REGION", "India")

    bq = bigquery.Client()

    # Get emission factors safely
    energy_factor, energy_ref = get_emission_factor(region, "electricity")
    transport_factor, transport_ref = get_emission_factor("Global", "freight_truck")

    # Fallback if missing
    if not energy_factor:
        energy_factor = 0.82
        energy_ref = "Default"

    if not transport_factor:
        transport_factor = 0.0525
        transport_ref = "Default"

    where = ""
    if since:
        where = f"WHERE record_date >= DATE('{since}')"

    query = f"""
    SELECT
      o.product_id,
      IFNULL(p.product_name, o.product_id) AS product_name,
      IFNULL(p.category, 'Unknown') AS category,
      SUM(o.units_sold) AS units,
      SUM(o.energy_kwh) AS energy,
      SUM(o.transport_km) AS km
    FROM sustainability_ds.operations o
    LEFT JOIN sustainability_ds.product_catalogue p
      ON o.product_id = p.product_id
    {where}
    GROUP BY o.product_id, product_name, category
    ORDER BY product_name
    """

    rows = bq.query(query).result()

    data = []

    for r in rows:

        units = r.units or 0
        energy = r.energy or 0
        km = r.km or 0

        energy_co2 = energy * energy_factor
        transport_co2 = km * transport_factor

        data.append({
            "product_id": r.product_id,
            "product_name": r.product_name,
            "category": r.category,
            "total_units_sold": int(units),
            "energy_co2_kg": float(energy_co2),
            "transport_co2_kg": float(transport_co2),
            "total_co2_kg": float(energy_co2 + transport_co2),
            "energy_ref": energy_ref,
            "transport_ref": transport_ref
        })

    return {"count": len(data), "data": data}


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

    prev_cpu = None

    for row in results:

        total = float(row.total_co2 or 0)
        units = float(row.total_units or 0)

        cpu = total / units if units > 0 else 0

        trend = None
        if prev_cpu is not None:
            trend = round(((cpu - prev_cpu) / prev_cpu) * 100, 2)

        prev_cpu = cpu

        data.append({
            "month": row.month,
            "co2_per_unit": round(cpu, 4),
            "efficiency_change": trend
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
def total_footprint():

    bq = bigquery.Client()

    query = """
    WITH product AS (
      SELECT
        FORMAT_DATE('%Y-%m', record_date) AS month,
        SUM(energy_kwh * 0.82 + transport_km * 0.0525) AS co2
      FROM sustainability_ds.operations
      GROUP BY month
    ),

    utility AS (
      SELECT
        FORMAT_DATE('%Y-%m', month) AS month,
        SUM(estimated_co2) AS co2
      FROM sustainability_ds.bill_emissions
      GROUP BY month
    )

    SELECT
      COALESCE(p.month, u.month) AS month,

      IFNULL(p.co2, 0) AS product_co2,
      IFNULL(u.co2, 0) AS utility_co2,

      IFNULL(p.co2, 0) + IFNULL(u.co2, 0) AS total_co2

    FROM product p
    FULL OUTER JOIN utility u
      ON p.month = u.month

    ORDER BY month;
    """

    rows = bq.query(query).result()

    data = []

    for r in rows:
        data.append({
            "month": r.month,
            "product_co2": float(r.product_co2),
            "utility_co2": float(r.utility_co2),
            "total_co2": float(r.total_co2)
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

def get_emission_factor(region, activity):

    bq = bigquery.Client()

    query = f"""
    SELECT factor, reference
    FROM sustainability_ds.emission_factors
    WHERE region = '{region}'
      AND activity_type = '{activity}'
    ORDER BY year DESC
    LIMIT 1
    """

    rows = list(bq.query(query).result())

    if not rows:
        return None, None

    return rows[0].factor, rows[0].reference

@app.get("/export/excel")
def export_excel():

    bq = bigquery.Client()

    # Get product metrics
    metrics_query = """
    SELECT
      o.product_id,
      IFNULL(p.product_name, o.product_id) AS product_name,
      IFNULL(p.category, 'Unknown') AS category,
      SUM(o.units_sold) AS units_sold,
      SUM(o.energy_kwh * 0.82) AS energy_co2,
      SUM(o.transport_km * 0.0525) AS transport_co2,
      SUM(o.energy_kwh * 0.82 + o.transport_km * 0.0525) AS total_co2
    FROM sustainability_ds.operations o
    LEFT JOIN sustainability_ds.product_catalogue p
      ON o.product_id = p.product_id
    GROUP BY o.product_id, product_name, category
    """

    rows = bq.query(metrics_query).to_dataframe()


    # Create Excel in memory
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        rows.to_excel(writer, index=False, sheet_name="Product_Emissions")

    output.seek(0)


    headers = {
        "Content-Disposition": "attachment; filename=carbon_report.xlsx"
    }

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )

@app.post("/export/pdf")
async def export_pdf(request: Request):

    data = await request.json()

    trend_img = data.get("trend")
    bill_img = data.get("bill")
    total_img = data.get("total")

    def decode_image(img_base64):

        if not img_base64:
            return None

        header, encoded = img_base64.split(",", 1)

        return io.BytesIO(base64.b64decode(encoded))


    trend_buffer = decode_image(trend_img)
    bill_buffer = decode_image(bill_img)
    total_buffer = decode_image(total_img)


    bq = bigquery.Client()

    query = """
    SELECT
      IFNULL(p.product_name, o.product_id) AS product,
      IFNULL(p.category, 'Unknown') AS category,
      SUM(o.units_sold) AS units,
      SUM(o.energy_kwh * 0.82) AS energy_co2,
      SUM(o.transport_km * 0.0525) AS transport_co2,
      SUM(o.energy_kwh * 0.82 + o.transport_km * 0.0525) AS total_co2
    FROM sustainability_ds.operations o
    LEFT JOIN sustainability_ds.product_catalogue p
      ON o.product_id = p.product_id
    GROUP BY product, category
    ORDER BY total_co2 DESC
    """

    rows = list(bq.query(query).result())


    buffer = io.BytesIO()

    doc = SimpleDocTemplate(buffer, pagesize=A4)

    styles = getSampleStyleSheet()
    elements = []


    # ---------- Title ----------
    elements.append(Paragraph(
        "Sustainability Emissions Report",
        styles["Title"]
    ))

    elements.append(Spacer(1, 15))


    # ---------- Date ----------
    elements.append(Paragraph(
        f"Generated on: {datetime.now().strftime('%d %B %Y, %H:%M')}",
        styles["Normal"]
    ))

    elements.append(Spacer(1, 20))


    # ---------- Charts ----------
    elements.append(Paragraph("Emission Trends", styles["Heading2"]))
    elements.append(Spacer(1, 10))

    if trend_buffer:
        elements.append(Image(trend_buffer, width=450, height=250))

    elements.append(Spacer(1, 20))


    elements.append(Paragraph("Utility Emissions", styles["Heading2"]))
    elements.append(Spacer(1, 10))

    if bill_buffer:
        elements.append(Image(bill_buffer, width=450, height=250))

    elements.append(Spacer(1, 20))


    elements.append(Paragraph("Total Carbon Footprint", styles["Heading2"]))
    elements.append(Spacer(1, 10))

    if total_buffer:
        elements.append(Image(total_buffer, width=450, height=250))

    elements.append(PageBreak())


    # ---------- Table ----------
    table_data = [[
        "Product","Category","Units",
        "Energy CO₂","Transport CO₂","Total CO₂"
    ]]

    for r in rows:
        table_data.append([
            r.product,
            r.category,
            str(int(r.units or 0)),
            f"{(r.energy_co2 or 0):.2f}",
            f"{(r.transport_co2 or 0):.2f}",
            f"{(r.total_co2 or 0):.2f}",
        ])


    table = Table(table_data, repeatRows=1)

    table.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),lightgrey),
        ("GRID",(0,0),(-1,-1),0.5,grey),
        ("FONT",(0,0),(-1,0),"Helvetica-Bold"),
        ("ALIGN",(2,1),(-1,-1),"CENTER"),
    ]))


    elements.append(table)


    elements.append(Spacer(1, 20))

    elements.append(Paragraph(
        "Generated by Sustainability Analytics Platform",
        styles["Italic"]
    ))


    doc.build(elements)

    buffer.seek(0)


    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={
            "Content-Disposition":
            "attachment; filename=sustainability_report.pdf"
        }
    )
