
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Numeric, VARCHAR
from datetime import datetime
import subprocess
import os

# 建立 SQL Server 和 MySQL 的資料庫連線
server_engine = create_engine('mssql+pyodbc://{account}:{password}@xxx.xxx.xx.xx/{dbname}?driver=ODBC+Driver+17+for+SQL+Server')
mysql_engine = create_engine('mysql+pymysql://{rootuser}:{password}@localhost:3309/{schemas_name}?charset=utf8mb4')

# 動態取得pk的方式
def get_primary_keys(table_name):
    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = '{table_name}'
    """
    pk_df = pd.read_sql(query, con=server_engine)
    return pk_df['COLUMN_NAME'].tolist()

# 動態生成型別的ㄏㄞ函式
def get_dtype_mapping(df: pd.DataFrame) -> dict:
    mapping = {}
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            mapping[col] = Integer()
        elif pd.api.types.is_float_dtype(dtype):
            mapping[col] = Numeric(10, 4)
        elif pd.api.types.is_string_dtype(dtype):
            max_length = df[col].astype(str).map(len).max() or 255
            mapping[col] = VARCHAR(max_length)
        else:
            mapping[col] = VARCHAR(255)
    return mapping

# 依據pk動態遷移表資料，一次抓50萬筆，避免巨量資料導致站存記憶體不構
def migrate_table_in_chunks(source_table, target_table, chunk_size=500000):
    try:
        primary_keys = get_primary_keys(source_table)
        if not primary_keys:
            raise ValueError(f"Table '{source_table}' does not have primary keys defined.")
        order_by_clause = ", ".join(primary_keys)

        offset = 0
        while True:
            query = f'''
            SELECT * 
            FROM {source_table}
            ORDER BY {order_by_clause}
            OFFSET {offset} ROWS 
            FETCH NEXT {chunk_size} ROWS ONLY;
            '''
            df = pd.read_sql_query(query, con=server_engine)

            if df.empty:
                print(f"All records from '{source_table}' have been migrated.")
                break

            dtype_mapping = get_dtype_mapping(df)
            df.to_sql(target_table, con=mysql_engine, if_exists='append', index=False, dtype=dtype_mapping, chunksize=1000)
            print(f"Batch of {len(df)} records from '{source_table}' migrated to '{target_table}'.")
            offset += chunk_size
    except Exception as e:
        print(f"Error occurred while migrating table '{source_table}': {e}")

# 要遷移的資料表清單
tables_to_migrate = [
    "xxx","xxx"
]

# 執行資料遷移
for table_name in tables_to_migrate:
    migrate_table_in_chunks(table_name, table_name.lower(), chunk_size=500000)

# 匯出資料庫並壓縮
def export_database():
    db_user = "xxx"
    db_password = "xxxxxxxxx"
    db_host = "localhost"
    db_port = "3309"
    db_name = "xxxx"

    now = datetime.now()
    year = now.strftime('%Y')
    month = f"{int(now.strftime('%m')) - 1:02}"
    if month == "00":
        year = str(int(year) - 1)
        month = "12"
    file_name = f"B0A2{year}{month}.sql"

    dump_cmd = [
        "mysqldump",
        f"--user={db_user}",
        f"--password={db_password}",
        f"--host={db_host}",
        f"--port={db_port}",
        db_name
    ]

    with open(file_name, "w") as f:
        try:
            subprocess.run(dump_cmd, stdout=f, check=True)
            print(f"資料庫匯出成功：{file_name}")
        except subprocess.CalledProcessError as e:
            print(f"匯出失敗：{e}")
            return

    rar_file_name = os.path.splitext(file_name)[0] + ".rar"
    try:
        rar_path = r"C:/Program Files/WinRAR/Rar.exe"
        subprocess.run([rar_path, "a", rar_file_name, file_name], check=True)
        print(f"資料庫匯出並壓縮成功：{rar_file_name}")
    except subprocess.CalledProcessError as e:
        print(f"壓縮失敗：{e}")

# 呼叫匯出函數
export_database()
