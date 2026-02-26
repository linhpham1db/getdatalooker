import pandas as pd
import gspread
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pymysql
import datetime
import tempfile
import json
import os

DB_HOST = "dbvms-read.cw37wboebtg4.us-east-2.rds.amazonaws.com"
DB_USER = "vietnguyen"
DB_PASSWORD = "DatBike@12345!"
DB_NAME = "datbikedb"
DB_PORT = 3306

SERVICE_ACCOUNT_KEY_PATH = "boreal-airway-473310-q3-56220fa7be41.json"
SERVICE_ACCOUNT_JSON = {
  "type": "service_account",
  "project_id": "boreal-airway-473310-q3",
  "private_key_id": "56220fa7be4185418022097b8b91f6028357f451",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCerZMzD8EIeL7c\n7PJ5tiQo6Ft6fGMwgWf/O8kWxyIy1OquGaRkWZPpzvsde8u9qGHLDJhSa9tavkzC\nso4RCWp5xLsP/MGN87t/+VdE3Aa6C3tBQcfdrbyBJVNXc+1lt48DcFukVh5L1G6t\n940VaMEIWBnez5jdyqiR9RSmOHH8Tt+2H839T0eDrrsZlVafsQK5X+YGJpHglWnL\nHZD7qN+AhaouQLPDpo75prRGJPC1hvCdSBApcpgxhf4NW1fcec4P7RjN+yd2Ms49\nRLCB2d7B2NHHjgHAytDlOmJyfjpKTyZ7NYOKEKWQvfSWwTYYi2cVyqpu1CNY9M/o\nJ+JEvHBLAgMBAAECggEAQSS+hVyG4az8oqOOaoRp9Pdrsu4FJMFDykenpzfKpPa/\ngAlb8azK6lbExlZwDyVlPKCnmlLYY7kzt0zpwTnN4j7vk0Evz1DMXd4mQ0lXY6La\nD7DrVmmAtb52qDUaNcO1rXI/1UhbuML2S2qRm8P9V6kfudcdiZStPvrQZutOdIfg\nuMeCtBPYO4Xvfg0lKsdoOisn8WHRUWMPjP89HPnCGMymLeVZy7jl07rUfWRrETrK\nMeRHAQFFD7JkC68gMKjUC0NXTIY3MORdbSntFCv6VG3euoFyfUwoj81dJv4DMGa8\nwrv7GBMfpkhp3t36EvRe50+bG4eXSEfz5AwLgrFCvQKBgQDfUitkHaPO458ibQDn\nn4J2zlN/afw54dZSdXM4tEiQ/Y44Io5qQsL4lyhhEPRWTf4sf6MQwx+XFBmqtoKp\n8uOlz77zJA00omcAnDDx1TMgw4bU+2WEe3FLO5P3D3/CMkUvP2BZrXQOGMVZ+Kue\nA3TpL7JnhmP9riAHUBTAweeqhwKBgQC15dCnBQY+eN4w0z3oqYZ1VYeMsoSVtL8S\nnWAlw0YwG0bidmFynISiWmZOwLOEdkFF0JlM21u0enpqJT1Mo93mycKnPMVgHiwx\nyUrygVlZUYLIVYuSUyUMp3BH4jbxQE+aWI41aryXrwEHIYXkkobKl0sSMhvxbq+e\nAEk3ln2pHQKBgBmbl4Qsbes8aLQO4cqtnM04zZRPt+9/OQ9Njn/TFHsjyBohxEjl\nDOxqkOPIiOwYl2vM2wsCBO5TDLppoUQRqQ8Lam8BFE6TzNHiFy6z7t/z69MiXLq7\nWPtygn4TFehT0WEgmFDQNf2j04WEoVFGPjK7GG8Mlwyw2dVc1nRgB8ujAoGAAu5R\nDl/mWdtYuTCuLrGMmRdnt7yopkDjU0l14yXiW6QU4FFIALDE8ljCjUJNojTFmHpj\n/fkK4T2X+13ePv3k9XMKz+cKxyG1VJgJvR6Ycff+Q3wdY54zWqDYDlB20ixXHAWR\nRQsTPt5zBcpkfepegaUucHyeNqWx7rnSdDLYk4ECgYBGYtpkrs0AeOw3vg3pLu/y\niej4mkE/KVpEnmADfnjruvwkEGfngdnjzMptIK/v4d8ng2nWdze3WHWXLCh6Kaut\nf86FRCEUf/XqlitBaIL/rqb10NrTWz7MbbptzuOi67dLy+vY0QzCqnW2GFle0Xjf\nqdstRkFn2avWkUuHOjPLeA==\n-----END PRIVATE KEY-----\n",
  "client_email": "push-vms-to-ggs@boreal-airway-473310-q3.iam.gserviceaccount.com",
  "client_id": "108713013004074742662",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/push-vms-to-ggs%40boreal-airway-473310-q3.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

SPREADSHEET_ID = "11JwOh8SvjahxZcbHPtr3IrOQGsd--zqq3P9HACdWgek"
SHEET_NAME = "db_vms"
SHEET_NAME_UP_VMS = "up_vms"
SHEET_NAME_SALE = "sale_vms"

# ======================================================================
# 2. TRUY VẤN SQL (Lấy dữ liệu inventory với frame number)
# ======================================================================
SQL_QUERY = """
    WITH LatestShippingStatus AS (
        SELECT
            se.shipping_id,
            se.status AS latest_shipping_status,
            ROW_NUMBER() OVER (PARTITION BY se.shipping_id ORDER BY se.created_at DESC) AS rn
        FROM
            shipping_event se
    ),
    MaxShippingForTransfer AS (
        SELECT
            sh.ref_id AS transfer_order_id,
            MAX(sh.id) AS max_shipping_id
        FROM
            shipping sh
        GROUP BY
            sh.ref_id
    )
    SELECT
        i.id,
        s_inv.name AS store_name,
        s_inv.type AS store_type,
        p.sku,
        p.name AS product_name,
        i.frame_number,
        i.motor_number,
        i.status AS inventory_status,
        i.quantity,
        i.created_at,
        to_.id AS transfer_id,
        to_.status AS transfer_status,
        to_.created_at AS transfer_created_at,
        to_.sent_at AS transfer_sent_at,
        to_.received_at AS transfer_received_at,
        to_.cancelled_at AS transfer_cancelled_at,
        msh.max_shipping_id AS shipping_id,
        lss.latest_shipping_status,
        -- Các cột mới được thêm/cập nhật: Tên và Loại kho nhận
        s_to.name AS to_store_name,
        s_to.type AS to_store_type
    FROM inventory i
    JOIN product p ON i.product_id = p.id
    JOIN store s_inv ON i.store_id = s_inv.id
    LEFT JOIN stock_transfer st
        ON i.id = st.from_inventory_id OR i.id = st.to_inventory_id
    LEFT JOIN transfer_order to_
        ON st.transfer_id = to_.id
    -- JOIN mới để lấy tên và loại kho nhận
    LEFT JOIN store s_to
        ON to_.to_store_id = s_to.id
    LEFT JOIN MaxShippingForTransfer msh
        ON to_.id = msh.transfer_order_id
    LEFT JOIN shipping sh
        ON msh.max_shipping_id = sh.id
    LEFT JOIN LatestShippingStatus lss
        ON msh.max_shipping_id = lss.shipping_id AND lss.rn = 1
    WHERE i.frame_number IS NOT NULL
    AND TRIM(i.frame_number) <> ''
    AND i.quantity > 0
    AND (to_.status IS NULL OR to_.status <> 'CANCELLED')
    ORDER BY i.created_at DESC;
"""

SQL_QUERY_UP_VMS = """
SELECT
    i.frame_number,
    i.motor_number,
    i.status,
    i.quantity,
    DATE(i.created_at) AS created_date,
    p.name AS product_name,
    s.name AS store_name,
    s.type AS store_type
FROM
    inventory i
JOIN
    product p ON i.product_id = p.id
JOIN
    store s ON i.store_id = s.id
WHERE
    i.store_id = 1
    AND i.frame_number IS NOT NULL
ORDER BY
    i.created_at DESC
LIMIT 1000;
"""

SQL_QUERY_SALE = """
SELECT
    oli.order_id,
    oli.frame_number,
    p.name AS product_name,
    so.order_status,
    DATE(DATE_ADD(so.paid_at, INTERVAL 7 HOUR)) AS paid_date,
    DATE(DATE_ADD(so.fulfilled_at, INTERVAL 7 HOUR)) AS fulfilled_date,
    p.category,
    s.name AS store_name,
    s.type AS store_type
FROM
    order_line_item oli
JOIN
    sales_order so ON oli.order_id = so.id
JOIN
    product p ON oli.product_id = p.id
JOIN
    store s ON so.fulfillment_store_id = s.id
WHERE
    p.category = 'BIKE'
    AND so.paid_at >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
    AND so.order_status NOT IN ('CANCELLED', 'PENDING');

"""

def main():
    encoded_password = quote_plus(DB_PASSWORD)
    connection_string = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)

    df = pd.read_sql(SQL_QUERY, engine)
    df_up_vms = pd.read_sql(SQL_QUERY_UP_VMS, engine)
    df_sale = pd.read_sql(SQL_QUERY_SALE, engine)
    db_vms_rows = len(df)
    up_vms_rows = len(df_up_vms)
    sale_vms_rows = len(df_sale)
    print(f"fetched db_vms: {db_vms_rows} rows; up_vms: {up_vms_rows} rows; sale_vms: {sale_vms_rows} rows")

    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].astype(str)
    for col in df_up_vms.select_dtypes(include=['datetime64[ns]']).columns:
        df_up_vms[col] = df_up_vms[col].astype(str)
    for col in df_sale.select_dtypes(include=['datetime64[ns]']).columns:
        df_sale[col] = df_sale[col].astype(str)

    def _to_serializable(value):
        if isinstance(value, (datetime.date, datetime.datetime, pd.Timestamp)):
            return value.isoformat()
        return value

    df = df.applymap(_to_serializable)
    df_up_vms = df_up_vms.applymap(_to_serializable)
    df_sale = df_sale.applymap(_to_serializable)

    df = df.fillna('')
    df_up_vms = df_up_vms.fillna('')
    df_sale = df_sale.fillna('')

    # Prefer embedded JSON; fall back to path if needed
    tmp_key_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump(SERVICE_ACCOUNT_JSON, f)
            tmp_key_path = f.name
        gc = gspread.service_account(filename=tmp_key_path)
    finally:
        if tmp_key_path and os.path.exists(tmp_key_path):
            os.unlink(tmp_key_path)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    worksheet = spreadsheet.worksheet(SHEET_NAME)
    worksheet.clear()
    headers = df.columns.tolist()
    data_values = df.values.tolist()
    data_to_upload = [headers] + data_values
    worksheet.update(range_name='A1', values=data_to_upload, value_input_option='USER_ENTERED')

    worksheet_up = spreadsheet.worksheet(SHEET_NAME_UP_VMS)
    worksheet_up.clear()
    headers_up = df_up_vms.columns.tolist()
    data_values_up = df_up_vms.values.tolist()
    data_to_upload_up = [headers_up] + data_values_up
    worksheet_up.update(range_name='A1', values=data_to_upload_up, value_input_option='USER_ENTERED')

    # sale_vms sheet (create if not exists)
    try:
        worksheet_sale = spreadsheet.worksheet(SHEET_NAME_SALE)
    except gspread.exceptions.WorksheetNotFound:
        headers_sale = df_sale.columns.tolist()
        cols = max(1, len(headers_sale))
        worksheet_sale = spreadsheet.add_worksheet(title=SHEET_NAME_SALE, rows="1000", cols=str(cols))
    worksheet_sale.clear()
    headers_sale = df_sale.columns.tolist()
    data_values_sale = df_sale.values.tolist()
    data_to_upload_sale = [headers_sale] + data_values_sale
    worksheet_sale.update(range_name='A1', values=data_to_upload_sale, value_input_option='USER_ENTERED')

    print(f"written db_vms: {db_vms_rows} rows; up_vms: {up_vms_rows} rows; sale_vms: {sale_vms_rows} rows")

    return {
        "db_vms_rows": db_vms_rows,
        "up_vms_rows": up_vms_rows,
        "sale_vms_rows": sale_vms_rows
    }

if __name__ == "__main__":
    main()
