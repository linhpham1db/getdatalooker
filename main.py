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

SERVICE_ACCOUNT_KEY_PATH = "gen-lang-client-0967854406-770a3cc9dca9.json"
SERVICE_ACCOUNT_JSON = {
  "type": "service_account",
  "project_id": "gen-lang-client-0967854406",
  "private_key_id": "770a3cc9dca9359d0fae6bcddb4aa47adb1da764",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDrelpi5/p3y3xo\n1Woznwg7e6e5VOflX03XX8Wijd39cIHmWUnmSAteLPbh51tCosIx8wxAa5ThbzMC\njb0Dd195TWiV2xRuREGkPEzWLXcRRW/X6rx9J5ko7PTqCmFjwEGRr+HWRCjbL8i5\npY4oJkubNTg3Eqrauljelf192R+gg5ucxJC52Dci4rXoI1CkCdJ4c/GqJxbYsAcy\nOkU71xia9UwmEPN5tfZzFw6f+pj2OYcfKI3nqPd9vmomTCNkBtuTfyNjXMvuutjQ\nN9px5CsVzKqP9caX+zAEbPWGOfbCZyHAp2ycNgXU2rnp72Gm2GoNI52m8od9du4i\nmTrOkCSHAgMBAAECggEAAZRC+enTWK1RKsTPnwQAgqGcKNaEbZnbhEe8pA/ufq4b\n+Ob9Y2F1Tg7gIvixuHst8TbPjLkL+Q0NWwWHUHgi+EXIH0UmWGz1wbuzyjXo2AFO\ntDR7Sh2TXd5k8BLcv1YBOpBi49R2L64c2v3cu7EyDZroQxpJcgkQ4oE//3GO0D0+\naPYkFPEJ+kH0OsDGl8y3qyrVHYPN0FencuMyx++EqPKNtF/MqxI1QnV3TE6v5Jd8\nN6/7R5qX7n1ej9P0qR6v05NEVQpX4QpsrJIsACIwL8aFezivCifYK+8xpHJN4ghJ\nhc+8i/Mwq227yOt7Xprm1GlRg1qQSaDaO2lLUzBWEQKBgQD9YDcURPf+ck71tPBx\nfptLS1oVybyjq/iadpeldZBoKpWGa44lQG8M+7Mf6Yjd2f0QawEOv4bAF5730T3J\nJ2npmBcu+ezdgWEM4gZgsIq+p0Z2vJ7OtqUlJHXbbIuyhdUQuw57Hs+ZUVfHWIq3\na8j+CB/kXX38wQrJwi1gHz8R1wKBgQDt6q8+qmTIY7rI0Qw5+BvVljzvL/iswUSB\nrZHs4AWv4GCOue0VXJbPcuPtrgHHLhKi3yRP7RX2sfYPfKeNKOXJWNb4mfjccBYa\nkWCdZl9aZV/mgWe1R6D95d5NRtqiZv4Vn829il4zEfv6oiE4Th/E9Oj5Qmu47m8i\nxE0i7BeM0QKBgQDkaILeg9nnh5ZEi0shlNdbhd78uzKRfSqL3BKSdquqK5FFbtni\nHPa/BnuQbg8SpzspPLzkVaWcru6ASiDfn+crTA7CK0zq1YHugibrrNo2SkcMLLcg\nc6fmFrskBfLdCSZsgaPpO3o7pQdzTg8mkETNM/fD/r/fRQp7nM8Y0rIhWwKBgCeD\nBj+eBMbS6T5YmXM3JUg/fzcp/F6UalAvB0ETo14mIWMStbEKg3FIX1olv93YZPfv\nnxQ3B6LEw1ynExx6Yk8iFfGLgKz7YHBhHG6HheZ5V4fsjdCpaK9B8b1buwullyT4\nOS71P9ezcOma63FyaAxJsDdVNJat4n8for/d/btBAoGBAIdXPuIbUE5NnDwrcsMM\nGw7BeDuH5WUxBq/RlEcwAUtdPdLeElySap5RZX7tBJhJebGE1dtA1UaI69J5X40t\nUg4uXO9eR4Na6xyVqNVqckAw3jvf2bkcDv32nSoNMN+4lm2agbjLwDklP/B1nS2n\nytjhpkZHeJNafFSm90ibJgdx\n-----END PRIVATE KEY-----\n",
  "client_email": "linh-app-server@gen-lang-client-0967854406.iam.gserviceaccount.com",
  "client_id": "112255235458662327226",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/linh-app-server%40gen-lang-client-0967854406.iam.gserviceaccount.com",
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
