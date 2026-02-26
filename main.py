import pandas as pd
import gspread
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pymysql
import datetime
import tempfile
import json
import os

# ======================================================================
# 1. THÔNG TIN CẤU HÌNH (THAY THẾ TẠI ĐÂY)
# ======================================================================

# --- Thông tin Database ---
DB_CONFIG = {
    "host": "YOUR_DB_HOST",
    "user": "YOUR_DB_USER",
    "password": "YOUR_DB_PASSWORD",
    "name": "YOUR_DB_NAME",
    "port": 3306
}

# --- Thông tin Google Sheets ---
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"
SHEET_NAME_DB = "db_vms"
SHEET_NAME_UP_VMS = "up_vms"
SHEET_NAME_SALE = "sale_vms"

# --- Google Service Account JSON ---
# Dán toàn bộ nội dung file JSON của bạn vào đây
SERVICE_ACCOUNT_JSON = {
  "type": "service_account",
  "project_id": "YOUR_PROJECT_ID",
  "private_key_id": "YOUR_PRIVATE_KEY_ID",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "YOUR_CLIENT_EMAIL",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "...",
  "universe_domain": "googleapis.com"
}

# ======================================================================
# 2. CÁC CÂU LỆNH SQL (GIỮ NGUYÊN NỘI DUNG GỐC)
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
        s_to.name AS to_store_name,
        s_to.type AS to_store_type
    FROM inventory i
    JOIN product p ON i.product_id = p.id
    JOIN store s_inv ON i.store_id = s_inv.id
    LEFT JOIN stock_transfer st 
        ON i.id = st.from_inventory_id OR i.id = st.to_inventory_id
    LEFT JOIN transfer_order to_ 
        ON st.transfer_id = to_.id
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

# ======================================================================
# 3. HÀM XỬ LÝ CHÍNH
# ======================================================================

def main():
    # 3.1. Kết nối Database
    encoded_password = quote_plus(DB_CONFIG["password"])
    connection_string = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{encoded_password}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"
    )
    engine = create_engine(connection_string)

    # 3.2. Truy vấn dữ liệu
    print("Đang truy vấn dữ liệu từ MySQL...")
    df = pd.read_sql(SQL_QUERY, engine)
    df_up_vms = pd.read_sql(SQL_QUERY_UP_VMS, engine)
    df_sale = pd.read_sql(SQL_QUERY_SALE, engine)
    
    counts = {
        "db_vms": len(df),
        "up_vms": len(df_up_vms),
        "sale_vms": len(df_sale)
    }
    print(f"Lấy dữ liệu thành công: {counts}")

    # 3.3. Dọn dẹp và chuẩn hóa dữ liệu
    def clean_dataframe(target_df):
        # Chuyển datetime sang string
        for col in target_df.select_dtypes(include=['datetime64[ns]', 'datetime64']).columns:
            target_df[col] = target_df[col].astype(str)
        
        # Xử lý các đối tượng date/datetime còn sót lại và fill null
        return target_df.applymap(
            lambda x: x.isoformat() if isinstance(x, (datetime.date, datetime.datetime, pd.Timestamp)) else x
        ).fillna('')

    df = clean_dataframe(df)
    df_up_vms = clean_dataframe(df_up_vms)
    df_sale = clean_dataframe(df_sale)

    # 3.4. Kết nối Google Sheets (Sử dụng file tạm cho Service Account)
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

    # 3.5. Hàm cập nhật Worksheet
    def push_to_worksheet(dataframe, sheet_name):
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' không tồn tại. Đang tạo mới...")
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols=str(max(1, len(dataframe.columns))))
        
        worksheet.clear()
        data_to_upload = [dataframe.columns.tolist()] + dataframe.values.tolist()
        worksheet.update(range_name='A1', values=data_to_upload, value_input_option='USER_ENTERED')
        print(f"Đã cập nhật sheet: {sheet_name}")

    # Thực hiện đẩy dữ liệu
    push_to_worksheet(df, SHEET_NAME_DB)
    push_to_worksheet(df_up_vms, SHEET_NAME_UP_VMS)
    push_to_worksheet(df_sale, SHEET_NAME_SALE)

    print("--- HOÀN THÀNH ---")
    return counts

if __name__ == "__main__":
    main()
