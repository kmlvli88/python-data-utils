# SQL Server ➜ MySQL 資料遷移與備份工具

此專案為一個實用的 Python 腳本，實現以下功能：
- 從 SQL Server 資料庫批次遷移資料至 MySQL（支援主鍵排序、動態型別對應）
- 使用 `mysqldump` 匯出 MySQL 資料庫成 `.sql`
- 自動使用 WinRAR 壓縮匯出檔案

## 使用技術
- Python (pandas, SQLAlchemy)
- pyodbc + pymysql
- subprocess + datetime + os

## 專案結構
```
├── sqlserver_to_mysql_migration.py     # 主程式
├── .env.example                         # 環境變數範本
├── .gitignore                           # 設定
└── README.md                            # 本說明文件
```

## 設定說明

請依照 `.env.example` 建立 `.env` 檔案，填入資料庫連線資訊：

```bash
cp .env.example .env
```

## 執行方式

```bash
python sqlserver_to_mysql_migration.py
```

---

## 注意事項
- 請自行安裝必要的 Python 套件（`pip install -r requirements.txt`）
- 請確認 WinRAR 已安裝且路徑正確
- 建議僅用於內部測試或自動備份環境

---

## 作者
Po Cheng Shih（施柏丞）｜資料工程 / 後端 / 自動化工具開發