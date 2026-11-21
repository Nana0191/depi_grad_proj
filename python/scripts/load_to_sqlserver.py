"""
load_to_sqlserver.py

Loads cleaned pipeline output into the OlistDW SQL Server star schema:
  - Full mode (default): clears target tables (preserving -1 members) and reloads.
  - Incremental mode: stages incoming/delta rows and upserts via SQL MERGE,
    and pushes order-level changes (status, delivery dates, late flag) to
    existing fact_sales rows.

Usage:
    python load_to_sqlserver.py "C:\\path\\to\\processed_data\\current"
    python load_to_sqlserver.py "C:\\path\\to\\processed_data\\runs\\<run_id>" --mode incremental

Environment variables (optional):
    OLIST_SERVER   default: localhost
    OLIST_DB       default: OlistDW
"""

import argparse
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL



ID_COLS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "product_id",
    "order_id",
    "seller_id",
]

ORDER_UPDATE_COLS = [
    "purchase_date_key",
    "delivered_date_key",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "approval_hours",
    "delivery_days",
    "delivery_delay_days",
    "is_late",
]

BOOL_MAP = {True: 1, False: 0, 1: 1, 0: 0, "True": 1, "False": 0, "1": 1, "0": 0}


SERVER = os.getenv("OLIST_SERVER", "sqlserver")   # we wont connect to the local but the sql server container
DATABASE = os.getenv("OLIST_DB", "OlistDW")  # this will determine  DB from the enviroment variable OLIST_DB
#f it doesnot exists it will use the default DB OlistDW


def make_engine():
  
    
    username = os.getenv("OLIST_USER", "sa")
    password = os.getenv("OLIST_PASSWORD")  #some config to enter the sql server

    if not password:
        raise ValueError("OLIST_PASSWORD environment variable is not set.") #cannot make connection without a password

    connection_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=SERVER,
    database=DATABASE,
    query={"driver": "ODBC Driver 17 for SQL Server"}
)
    #and this is how to make connection we need middle between this and the db which is the driver

    return create_engine(connection_url, fast_executemany=True)
def to_date_key(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    formatted = dt.dt.strftime("%Y%m%d")
    return pd.to_numeric(formatted, errors="coerce").astype("Int64")


def resolve_csv_path(data_dir: str, delta_name: str, full_name: str, required: bool = True) -> str | None:
    delta_path = os.path.join(data_dir, delta_name)
    full_path = os.path.join(data_dir, full_name)
    if os.path.exists(delta_path):
        return delta_path
    if os.path.exists(full_path):
        return full_path
    if required:
        raise FileNotFoundError(f"Neither '{delta_path}' nor '{full_path}' was found.")
    return None


def read_optional_csv(path: str | None) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype={c: str for c in ID_COLS})


def clear_tables(engine):
    with engine.begin() as conn:
        dates = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar()
        if not dates:
            sys.exit("dim_date is empty. Run 02_populate_dim_date.sql first.")

        conn.execute(text("DELETE FROM fact_sales"))
        conn.execute(text("DELETE FROM dim_customers"))
        conn.execute(text("DELETE FROM dim_products WHERE product_key > 0"))
        conn.execute(text("DELETE FROM dim_sellers WHERE seller_key > 0"))


def ensure_dim_date_ready(engine):
    with engine.connect() as conn:
        dates = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar()
        if not dates:
            sys.exit("dim_date is empty. Run 02_populate_dim_date.sql first.")


# ---------------------------------------------------------------- customers
def prepare_customers(df_cust: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    if df_cust.empty:
        return pd.DataFrame(
            columns=["customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
        ), {}

    cust = df_cust.copy()
    if "customer_unique_id" not in cust.columns:
        cust["customer_unique_id"] = cust["customer_id"]

    order_cust_to_uid = {
        str(cid).strip(): str(uid).strip()
        for cid, uid in zip(cust["customer_id"], cust["customer_unique_id"])
        if pd.notna(cid) and pd.notna(uid)
    }

    dim_c = (
        cust.drop_duplicates(subset=["customer_unique_id"], keep="last")[
            [
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
            ]
        ]
        .copy()
    )
    dim_c["customer_unique_id"] = dim_c["customer_unique_id"].astype(str).str.strip()
    # restore leading zeros lost in CSV round-trips (char(5) column)
    dim_c["customer_zip_code_prefix"] = (
        dim_c["customer_zip_code_prefix"].astype("string").str.strip().str.zfill(5)
    )
    return dim_c, order_cust_to_uid


def load_dim_customers_full(dim_c: pd.DataFrame, order_cust_to_uid: dict[str, str], engine) -> dict[str, int]:
    if not dim_c.empty:
        dim_c.to_sql("dim_customers", engine, if_exists="append", index=False, chunksize=5000)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT customer_key, customer_unique_id FROM dim_customers")).fetchall()

    uid_to_key = {r.customer_unique_id.strip(): int(r.customer_key) for r in rows}
    cust_map = {cid: uid_to_key[uid] for cid, uid in order_cust_to_uid.items() if uid in uid_to_key}
    print(f"{'dim_customers':18s} loaded {len(dim_c):>6} unique customers")
    return cust_map


def merge_dim_customers(dim_c: pd.DataFrame, order_cust_to_uid: dict[str, str], engine) -> dict[str, int]:
    if not dim_c.empty:
        with engine.begin() as conn:
            conn.execute(text("""
                IF OBJECT_ID('tempdb..#stg_customers') IS NOT NULL DROP TABLE #stg_customers;
                CREATE TABLE #stg_customers (
                    customer_unique_id       char(32)    NOT NULL,
                    customer_zip_code_prefix char(5)     NULL,
                    customer_city            varchar(50) NULL,
                    customer_state           char(2)     NULL
                );
            """))
            dim_c.to_sql("#stg_customers", conn, if_exists="append", index=False, chunksize=5000)
            conn.execute(text("""
                MERGE dim_customers AS target
                USING #stg_customers AS src
                   ON target.customer_unique_id = src.customer_unique_id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.customer_zip_code_prefix = COALESCE(src.customer_zip_code_prefix, target.customer_zip_code_prefix),
                        target.customer_city            = COALESCE(src.customer_city, target.customer_city),
                        target.customer_state           = COALESCE(src.customer_state, target.customer_state),
                        target.loaded_at                = SYSUTCDATETIME()
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                    VALUES (src.customer_unique_id, src.customer_zip_code_prefix, src.customer_city, src.customer_state);
                DROP TABLE #stg_customers;
            """))

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT customer_key, customer_unique_id FROM dim_customers")).fetchall()

    uid_to_key = {r.customer_unique_id.strip(): int(r.customer_key) for r in rows}
    cust_map = {cid: uid_to_key[uid] for cid, uid in order_cust_to_uid.items() if uid in uid_to_key}
    print(f"{'dim_customers':18s} merged {len(dim_c):>6} delta rows")
    return cust_map


# ----------------------------------------------------------------- products
def to_nullable_int(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cast columns to nullable Int64 so 57.0 is sent as 57 (SQL Server int columns reject '57.0')."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").round().astype("Int64")
    return df


def prepare_products(df_prod: pd.DataFrame) -> pd.DataFrame:
    if df_prod.empty:
        return pd.DataFrame()

    cols = [
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "category_status",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "has_missing_dimensions",
        "volume_cm3",
    ]
    available_cols = [c for c in cols if c in df_prod.columns]
    dim_p = df_prod[available_cols].drop_duplicates(subset=["product_id"], keep="last").copy()
    dim_p["product_id"] = dim_p["product_id"].astype(str).str.strip()
    dim_p = dim_p[dim_p["product_id"] != "UNKNOWN"]

    if "category_status" not in dim_p.columns:
        dim_p["category_status"] = dim_p.get("product_category_name_english").notna().map(
            {True: "known", False: "unknown"}
        )
    if "has_missing_dimensions" not in dim_p.columns:
        dim_p["has_missing_dimensions"] = 0

    dim_p = to_nullable_int(dim_p, [
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm",
        "product_width_cm", "volume_cm3",
    ])
    dim_p["has_missing_dimensions"] = (
        pd.to_numeric(dim_p["has_missing_dimensions"], errors="coerce").fillna(0).astype(int)
    )
    return dim_p


def load_dim_products_full(dim_p: pd.DataFrame, engine) -> dict[str, int]:
    if not dim_p.empty:
        dim_p.to_sql("dim_products", engine, if_exists="append", index=False, chunksize=5000)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT product_key, product_id FROM dim_products")).fetchall()

    prod_map = {r.product_id.strip(): int(r.product_key) for r in rows}
    print(f"{'dim_products':18s} loaded {len(dim_p):>6} products")
    return prod_map


def merge_dim_products(dim_p: pd.DataFrame, engine) -> dict[str, int]:
    if not dim_p.empty:
        with engine.begin() as conn:
            conn.execute(text("""
                IF OBJECT_ID('tempdb..#stg_products') IS NOT NULL DROP TABLE #stg_products;
                CREATE TABLE #stg_products (
                    product_id                    char(32)    NOT NULL,
                    product_category_name         varchar(50) NULL,
                    product_category_name_english varchar(50) NULL,
                    category_status               varchar(20) NOT NULL,
                    product_name_length           int         NULL,
                    product_description_length    int         NULL,
                    product_photos_qty            int         NULL,
                    product_weight_g              int         NULL,
                    product_length_cm             int         NULL,
                    product_height_cm             int         NULL,
                    product_width_cm              int         NULL,
                    has_missing_dimensions        bit         NOT NULL,
                    volume_cm3                    int         NULL
                );
            """))
            dim_p.to_sql("#stg_products", conn, if_exists="append", index=False, chunksize=5000)
            conn.execute(text("""
                MERGE dim_products AS target
                USING #stg_products AS src
                   ON target.product_id = src.product_id
                  AND target.product_key > 0
                WHEN MATCHED THEN
                    UPDATE SET
                        target.product_category_name         = src.product_category_name,
                        target.product_category_name_english = src.product_category_name_english,
                        target.category_status               = src.category_status,
                        target.product_name_length           = src.product_name_length,
                        target.product_description_length    = src.product_description_length,
                        target.product_photos_qty            = src.product_photos_qty,
                        target.product_weight_g              = src.product_weight_g,
                        target.product_length_cm             = src.product_length_cm,
                        target.product_height_cm             = src.product_height_cm,
                        target.product_width_cm              = src.product_width_cm,
                        target.has_missing_dimensions        = src.has_missing_dimensions,
                        target.volume_cm3                    = src.volume_cm3,
                        target.loaded_at                     = SYSUTCDATETIME()
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        product_id, product_category_name, product_category_name_english,
                        category_status, product_name_length, product_description_length,
                        product_photos_qty, product_weight_g, product_length_cm,
                        product_height_cm, product_width_cm, has_missing_dimensions, volume_cm3
                    )
                    VALUES (
                        src.product_id, src.product_category_name, src.product_category_name_english,
                        src.category_status, src.product_name_length, src.product_description_length,
                        src.product_photos_qty, src.product_weight_g, src.product_length_cm,
                        src.product_height_cm, src.product_width_cm, src.has_missing_dimensions, src.volume_cm3
                    );
                DROP TABLE #stg_products;
            """))

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT product_key, product_id FROM dim_products")).fetchall()

    prod_map = {r.product_id.strip(): int(r.product_key) for r in rows}
    print(f"{'dim_products':18s} merged {len(dim_p):>6} delta rows")
    return prod_map


# ------------------------------------------------------------------ sellers
def prepare_sellers(df_items: pd.DataFrame) -> list[str]:
    if df_items.empty or "seller_id" not in df_items.columns:
        return []
    seller_ids = df_items["seller_id"].dropna().astype(str).str.strip().unique().tolist()
    return [s for s in seller_ids if s and s.upper() != "UNKNOWN"]


def load_dim_sellers_full(seller_ids: list[str], engine) -> dict[str, int]:
    if seller_ids:
        dim_s = pd.DataFrame({"seller_id": seller_ids})
        dim_s.to_sql("dim_sellers", engine, if_exists="append", index=False, chunksize=5000)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT seller_key, seller_id FROM dim_sellers")).fetchall()

    seller_map = {r.seller_id.strip(): int(r.seller_key) for r in rows}
    print(f"{'dim_sellers':18s} loaded {len(seller_ids):>6} sellers")
    return seller_map


def merge_dim_sellers(seller_ids: list[str], engine) -> dict[str, int]:
    if seller_ids:
        dim_s = pd.DataFrame({"seller_id": seller_ids})
        with engine.begin() as conn:
            conn.execute(text("""
                IF OBJECT_ID('tempdb..#stg_sellers') IS NOT NULL DROP TABLE #stg_sellers;
                CREATE TABLE #stg_sellers (
                    seller_id char(32) NOT NULL
                );
            """))
            dim_s.to_sql("#stg_sellers", conn, if_exists="append", index=False, chunksize=5000)
            conn.execute(text("""
                MERGE dim_sellers AS target
                USING #stg_sellers AS src
                   ON target.seller_id = src.seller_id
                  AND target.seller_key > 0
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (seller_id)
                    VALUES (src.seller_id);
                DROP TABLE #stg_sellers;
            """))

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT seller_key, seller_id FROM dim_sellers")).fetchall()

    seller_map = {r.seller_id.strip(): int(r.seller_key) for r in rows}
    print(f"{'dim_sellers':18s} merged {len(seller_ids):>6} delta sellers")
    return seller_map


# --------------------------------------------------------------------- fact
def build_fact_sales_df(
    df_orders: pd.DataFrame,
    df_items: pd.DataFrame,
    cust_map: dict[str, int],
    prod_map: dict[str, int],
    seller_map: dict[str, int],
    engine,
    mode: str,
) -> pd.DataFrame:
    if df_orders.empty and df_items.empty:
        return pd.DataFrame()

    orders = df_orders.copy()
    items = df_items.copy()

    if not orders.empty:
        orders["purchase_date_key"] = (
            orders["purchase_date_key"]
            if "purchase_date_key" in orders.columns
            else to_date_key(orders["order_purchase_timestamp"])
        )
        orders["delivered_date_key"] = (
            orders["delivered_date_key"]
            if "delivered_date_key" in orders.columns
            else to_date_key(orders["order_delivered_customer_date"])
        )

    order_cols = [
        "order_id",
        "customer_id",
        "purchase_date_key",
        "delivered_date_key",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "approval_hours",
        "delivery_days",
        "delivery_delay_days",
        "is_late",
    ]
    available_order_cols = [c for c in order_cols if c in orders.columns]
    orders_slim = (
        orders[available_order_cols].drop_duplicates(subset=["order_id"], keep="last")
        if not orders.empty
        else pd.DataFrame(columns=available_order_cols)
    )

    if not items.empty:
        items["order_item_id"] = pd.to_numeric(items["order_item_id"], errors="coerce").fillna(1).astype(int)
        items["price"] = pd.to_numeric(items["price"], errors="coerce").fillna(0.0).round(2)
        items["freight_value"] = pd.to_numeric(items["freight_value"], errors="coerce").fillna(0.0).round(2)
        if "item_total" not in items.columns:
            items["item_total"] = (items["price"] + items["freight_value"]).round(2)
        else:
            items["item_total"] = pd.to_numeric(items["item_total"], errors="coerce").fillna(
                items["price"] + items["freight_value"]
            ).round(2)

        merged = items.merge(orders_slim, on="order_id", how="inner")
        if len(merged) != len(items):
            print(
                f"WARNING: {len(items) - len(merged)} item rows have no matching order in the "
                "orders file and were skipped"
            )
    else:
        merged = pd.DataFrame()

    if not merged.empty:
        merged["customer_key"] = merged["customer_id"].astype(str).str.strip().map(cust_map)
        merged["product_key"] = merged["product_id"].astype(str).str.strip().map(prod_map).fillna(-1).astype(int)
        merged["seller_key"] = merged["seller_id"].astype(str).str.strip().map(seller_map).fillna(-1).astype(int)

    orders_with_items = set(items["order_id"].dropna().astype(str).str.strip().unique()) if not items.empty else set()
    no_item_orders = (
        orders_slim[~orders_slim["order_id"].astype(str).str.strip().isin(orders_with_items)].copy()
        if not orders_slim.empty
        else pd.DataFrame()
    )

    # Incremental: only create placeholder rows for orders not already in fact_sales
    # (with or without items). Changes to existing orders go through update_existing_orders().
    if mode == "incremental" and not no_item_orders.empty:
        with engine.connect() as conn:
            existing_orders = {
                str(r[0]).strip()
                for r in conn.execute(text("SELECT DISTINCT order_id FROM fact_sales")).fetchall()
            }
        no_item_orders = no_item_orders[
            ~no_item_orders["order_id"].astype(str).str.strip().isin(existing_orders)
        ]

    if not no_item_orders.empty:
        no_item_orders["order_item_id"] = 0
        no_item_orders["product_key"] = -1
        no_item_orders["seller_key"] = -1
        no_item_orders["price"] = 0.00
        no_item_orders["freight_value"] = 0.00
        no_item_orders["item_total"] = 0.00
        no_item_orders["shipping_limit_date"] = pd.NaT
        no_item_orders["customer_key"] = no_item_orders["customer_id"].astype(str).str.strip().map(cust_map)

    frames = [f for f in [merged, no_item_orders] if not f.empty]
    if not frames:
        return pd.DataFrame()

    fact_df = pd.concat(frames, ignore_index=True)

    before = len(fact_df)
    fact_df = fact_df.dropna(subset=["customer_key", "purchase_date_key"])
    if before != len(fact_df):
        print(f"WARNING: dropped {before - len(fact_df)} fact rows with no customer_key/purchase_date_key")

    fact_df["customer_key"] = fact_df["customer_key"].astype(int)
    fact_df["purchase_date_key"] = fact_df["purchase_date_key"].astype(int)
    if "delivered_date_key" in fact_df.columns:
        fact_df["delivered_date_key"] = pd.to_numeric(fact_df["delivered_date_key"], errors="coerce").astype("Int64")

    for dt_col in [
        "shipping_limit_date",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        if dt_col in fact_df.columns:
            fact_df[dt_col] = pd.to_datetime(fact_df[dt_col], errors="coerce")

    if "is_late" in fact_df.columns:
        fact_df["is_late"] = fact_df["is_late"].map(BOOL_MAP).astype("Int64")

    fact_df = to_nullable_int(fact_df, ["delivery_days", "delivery_delay_days"])

    fact_cols = [
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "seller_key",
        "purchase_date_key",
        "delivered_date_key",
        "price",
        "freight_value",
        "item_total",
        "shipping_limit_date",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "approval_hours",
        "delivery_days",
        "delivery_delay_days",
        "is_late",
    ]
    final_cols = [c for c in fact_cols if c in fact_df.columns]
    fact_df = fact_df[final_cols].drop_duplicates(subset=["order_id", "order_item_id"], keep="last")
    return fact_df


def load_fact_sales_full(fact_df: pd.DataFrame, engine):
    if not fact_df.empty:
        fact_df.to_sql("fact_sales", engine, if_exists="append", index=False, chunksize=5000)
    print(f"{'fact_sales':18s} loaded {len(fact_df):>6} rows")


def merge_fact_sales(fact_df: pd.DataFrame, engine):
    if fact_df.empty:
        print(f"{'fact_sales':18s} merged      0 delta rows")
        return

    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('tempdb..#stg_fact_sales') IS NOT NULL DROP TABLE #stg_fact_sales;
            CREATE TABLE #stg_fact_sales (
                order_id                      char(32)      NOT NULL,
                order_item_id                 int           NOT NULL,
                customer_key                  int           NOT NULL,
                product_key                   int           NOT NULL,
                seller_key                    int           NOT NULL,
                purchase_date_key             int           NOT NULL,
                delivered_date_key            int           NULL,
                price                         decimal(12,2) NOT NULL,
                freight_value                 decimal(12,2) NOT NULL,
                item_total                    decimal(12,2) NOT NULL,
                shipping_limit_date           datetime2     NULL,
                order_status                  varchar(20)   NULL,
                order_purchase_timestamp      datetime2     NULL,
                order_approved_at             datetime2     NULL,
                order_delivered_carrier_date  datetime2     NULL,
                order_delivered_customer_date datetime2     NULL,
                order_estimated_delivery_date date          NULL,
                approval_hours                decimal(10,2) NULL,
                delivery_days                 int           NULL,
                delivery_delay_days           int           NULL,
                is_late                       bit           NULL
            );
        """))
        fact_df.to_sql("#stg_fact_sales", conn, if_exists="append", index=False, chunksize=5000)
        conn.execute(text("""
            MERGE fact_sales AS target
            USING #stg_fact_sales AS src
               ON target.order_id = src.order_id
              AND target.order_item_id = src.order_item_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.customer_key                  = src.customer_key,
                    target.product_key                   = src.product_key,
                    target.seller_key                    = src.seller_key,
                    target.purchase_date_key             = src.purchase_date_key,
                    target.delivered_date_key            = src.delivered_date_key,
                    target.price                         = src.price,
                    target.freight_value                 = src.freight_value,
                    target.item_total                    = src.item_total,
                    target.shipping_limit_date           = src.shipping_limit_date,
                    target.order_status                  = src.order_status,
                    target.order_purchase_timestamp      = src.order_purchase_timestamp,
                    target.order_approved_at             = src.order_approved_at,
                    target.order_delivered_carrier_date  = src.order_delivered_carrier_date,
                    target.order_delivered_customer_date = src.order_delivered_customer_date,
                    target.order_estimated_delivery_date = src.order_estimated_delivery_date,
                    target.approval_hours                = src.approval_hours,
                    target.delivery_days                 = src.delivery_days,
                    target.delivery_delay_days           = src.delivery_delay_days,
                    target.is_late                       = src.is_late,
                    target.loaded_at                     = SYSUTCDATETIME()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    order_id, order_item_id, customer_key, product_key, seller_key,
                    purchase_date_key, delivered_date_key, price, freight_value, item_total,
                    shipping_limit_date, order_status, order_purchase_timestamp,
                    order_approved_at, order_delivered_carrier_date,
                    order_delivered_customer_date, order_estimated_delivery_date,
                    approval_hours, delivery_days, delivery_delay_days, is_late
                )
                VALUES (
                    src.order_id, src.order_item_id, src.customer_key, src.product_key, src.seller_key,
                    src.purchase_date_key, src.delivered_date_key, src.price, src.freight_value, src.item_total,
                    src.shipping_limit_date, src.order_status, src.order_purchase_timestamp,
                    src.order_approved_at, src.order_delivered_carrier_date,
                    src.order_delivered_customer_date, src.order_estimated_delivery_date,
                    src.approval_hours, src.delivery_days, src.delivery_delay_days, src.is_late
                );
            DROP TABLE #stg_fact_sales;
        """))

    print(f"{'fact_sales':18s} merged {len(fact_df):>6} delta rows")


def update_existing_orders(df_orders: pd.DataFrame, engine):
    """Push order-level changes (status, delivery dates, late flag) to ALL fact_sales rows of each order."""
    if df_orders.empty:
        print(f"{'order updates':18s} updated      0 fact rows")
        return

    o = df_orders.copy()
    o["order_id"] = o["order_id"].astype(str).str.strip()
    if "purchase_date_key" not in o.columns:
        o["purchase_date_key"] = to_date_key(o["order_purchase_timestamp"])
    if "delivered_date_key" not in o.columns:
        o["delivered_date_key"] = to_date_key(o["order_delivered_customer_date"])
    o = o.drop_duplicates(subset=["order_id"], keep="last")

    for c in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        if c in o.columns:
            o[c] = pd.to_datetime(o[c], errors="coerce")
    for c in ["purchase_date_key", "delivered_date_key"]:
        o[c] = pd.to_numeric(o[c], errors="coerce").astype("Int64")
    if "is_late" in o.columns:
        o["is_late"] = o["is_late"].map(BOOL_MAP).astype("Int64")

    cols = [c for c in ORDER_UPDATE_COLS if c in o.columns]
    o = o[["order_id"] + cols]
    set_sql = ",\n                ".join(f"f.{c} = s.{c}" for c in cols)

    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('tempdb..#stg_order_upd') IS NOT NULL DROP TABLE #stg_order_upd;"))
        o.to_sql("#stg_order_upd", conn, if_exists="replace", index=False, chunksize=5000)
        res = conn.execute(text(f"""
            UPDATE f SET
                {set_sql},
                f.loaded_at = SYSUTCDATETIME()
            FROM fact_sales f
            JOIN #stg_order_upd s ON s.order_id = f.order_id;
        """))
        conn.execute(text("DROP TABLE #stg_order_upd;"))

    print(f"{'order updates':18s} updated {res.rowcount:>6} fact rows")


# ----------------------------------------------------------------------- cli
def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Olist star schema into SQL Server.")
    parser.add_argument(
        "data_dir",
        nargs="?",
        default=os.path.join("processed_data", "current"),
        help="Folder containing CSV or delta_*.csv files",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Load mode: 'full' (delete & reload) or 'incremental' (MERGE)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    engine = make_engine()

    if args.mode == "full":
        cust_path = os.path.join(args.data_dir, "dim_customers.csv")
        prod_path = os.path.join(args.data_dir, "dim_products.csv")
        orders_path = os.path.join(args.data_dir, "fact_orders.csv")
        items_path = os.path.join(args.data_dir, "fact_order_items.csv")

        for path in [cust_path, prod_path, orders_path, items_path]:
              if not os.path.exists(path):
                   raise FileNotFoundError(f"Required full-load file not found: {path}")

        df_cust = pd.read_csv(cust_path, dtype={c: str for c in ID_COLS})
        df_prod = pd.read_csv(prod_path, dtype={c: str for c in ID_COLS})
        df_orders = pd.read_csv(orders_path, dtype={c: str for c in ID_COLS})
        df_items = pd.read_csv(items_path, dtype={c: str for c in ID_COLS})

        clear_tables(engine)
        dim_c, order_cust_to_uid = prepare_customers(df_cust)
        cust_map = load_dim_customers_full(dim_c, order_cust_to_uid, engine)
        prod_map = load_dim_products_full(prepare_products(df_prod), engine)
        seller_map = load_dim_sellers_full(prepare_sellers(df_items), engine)
        fact_df = build_fact_sales_df(df_orders, df_items, cust_map, prod_map, seller_map, engine, mode="full")
        load_fact_sales_full(fact_df, engine)
    else:
        ensure_dim_date_ready(engine)
        cust_path = resolve_csv_path(args.data_dir, "delta_dim_customers.csv", "dim_customers.csv", required=False)
        prod_path = resolve_csv_path(args.data_dir, "delta_dim_products.csv", "dim_products.csv", required=False)
        orders_path = resolve_csv_path(args.data_dir, "delta_fact_orders.csv", "fact_orders.csv", required=False)
        items_path = resolve_csv_path(args.data_dir, "delta_fact_order_items.csv", "fact_order_items.csv", required=False)

        df_cust = read_optional_csv(cust_path)
        df_prod = read_optional_csv(prod_path)
        df_orders = read_optional_csv(orders_path)
        df_items = read_optional_csv(items_path)

        dim_c, order_cust_to_uid = prepare_customers(df_cust)
        cust_map = merge_dim_customers(dim_c, order_cust_to_uid, engine)
        prod_map = merge_dim_products(prepare_products(df_prod), engine)
        seller_map = merge_dim_sellers(prepare_sellers(df_items), engine)
        fact_df = build_fact_sales_df(df_orders, df_items, cust_map, prod_map, seller_map, engine, mode="incremental")
        merge_fact_sales(fact_df, engine)
        update_existing_orders(df_orders, engine)


if __name__ == "__main__":
    main()