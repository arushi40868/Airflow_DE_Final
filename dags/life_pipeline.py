from __future__ import annotations

# === Standard Library ===
import csv
import logging
import os
import shutil
from datetime import datetime, timedelta

# === Airflow ===
from airflow import DAG
from airflow.sdk import task  # Airflow 3+ TaskFlow API
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

# === Third-Party ===
from psycopg2 import Error as DatabaseError
from faker import Faker

# ------------------------------------------------------------------------------
# CONFIG (overridable via environment variables)
# ------------------------------------------------------------------------------
OUTPUT_DIR = os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")
PG_CONN_ID = os.environ.get("PG_CONN_ID", "Postgres")
SCHEMA = os.environ.get("PG_SCHEMA", "week8_demo")
TARGET_TABLE = os.environ.get("PG_TABLE", "employees")
APPEND = os.environ.get("PG_APPEND", "true").lower() == "true"

PERSON_QTY = int(os.environ.get("PERSON_QTY", "100"))
COMPANY_QTY = int(os.environ.get("COMPANY_QTY", "100"))

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ------------------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------------------
with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@daily",  # requirement: has a recurring schedule
    catchup=False,
    default_args=default_args,
    tags=["demo", "etl", "postgres"],
) as dag:

    # --- Utility: ensure staging dir exists ---
    @task()
    def ensure_output_dir(path: str = OUTPUT_DIR) -> str:
        os.makedirs(path, exist_ok=True)
        logging.info("Ensured output directory exists: %s", path)
        return path

    # ------------------------------------------------------------------------------
    # INGEST
    # ------------------------------------------------------------------------------
    @task()
    def fetch_persons(output_dir: str, quantity: int = PERSON_QTY) -> str:
        fake = Faker()
        data = []
        for _ in range(quantity):
            data.append(
                {
                    "firstname": fake.first_name(),
                    "lastname": fake.last_name(),
                    "email": fake.free_email(),
                    "phone": fake.phone_number(),
                    "address": fake.street_address(),
                    "city": fake.city(),
                    "country": fake.country(),
                }
            )

        filepath = os.path.join(output_dir, "persons.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        logging.info("Generated %d persons → %s", len(data), filepath)
        return filepath

    @task()
    def fetch_companies(output_dir: str, quantity: int = COMPANY_QTY) -> str:
        fake = Faker()
        data = []
        for _ in range(quantity):
            domain = fake.domain_name()
            data.append(
                {
                    "name": fake.company(),
                    "domain": domain,  # explicit relation key
                    "email": f"info@{domain}",
                    "phone": fake.phone_number(),
                    "country": fake.country(),
                    "website": f"https://{domain}",
                    "industry": fake.bs().split()[0].capitalize(),
                    "catch_phrase": fake.catch_phrase(),
                    "employees_count": fake.random_int(min=10, max=5000),
                    "founded_year": fake.year(),
                }
            )

        filepath = os.path.join(output_dir, "companies.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        logging.info("Generated %d companies → %s", len(data), filepath)
        return filepath

    # ------------------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------------------
    @task()
    def transform_persons(persons_csv: str) -> str:
        rows = []
        out = persons_csv.replace("persons.csv", "persons_tx.csv")
        with open(persons_csv, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for rec in r:
                email = (rec.get("email", "") or "").strip().lower()
                domain = email.split("@")[-1] if "@" in email else ""
                rows.append(
                    {
                        **rec,
                        "email": email,  # normalized
                        "person_domain": domain,  # join key
                    }
                )

        if not rows:
            raise ValueError("No person rows after transform.")

        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
        logging.info("Transformed persons → %s", out)
        return out

    @task()
    def transform_companies(companies_csv: str) -> str:
        rows = []
        out = companies_csv.replace("companies.csv", "companies_tx.csv")
        with open(companies_csv, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for rec in r:
                email = (rec.get("email", "") or "").strip().lower()
                employees = int(rec.get("employees_count") or 0)
                bucket = (
                    "1–99"
                    if employees < 100
                    else "100–999" if employees < 1000 else "1000+"
                )
                rows.append(
                    {
                        **rec,
                        "email": email,  # normalized
                        "employees_bucket": bucket,  # simple derived feature
                    }
                )

        if not rows:
            raise ValueError("No company rows after transform.")

        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
        logging.info("Transformed companies → %s", out)
        return out

    # ------------------------------------------------------------------------------
    # MERGE (true relation on domain)
    # ------------------------------------------------------------------------------
    @task()
    def merge_csvs_tx(persons_tx: str, companies_tx: str, output_dir: str) -> str:
        merged_path = os.path.join(output_dir, "merged_data.csv")

        with open(persons_tx, newline="", encoding="utf-8") as f1, open(
            companies_tx, newline="", encoding="utf-8"
        ) as f2:
            persons = list(csv.DictReader(f1))
            companies = list(csv.DictReader(f2))

        cx = {c.get("domain", ""): c for c in companies if c.get("domain")}
        merged = []
        for p in persons:
            dom = p.get("person_domain", "")
            c = cx.get(dom)
            if not c:
                continue  # skip if no matching company domain
            merged.append(
                {
                    "firstname": p["firstname"],
                    "lastname": p["lastname"],
                    "person_email": p["email"],
                    "company_name": c["name"],
                    "company_domain": c["domain"],
                    "company_email": c["email"],
                    "industry": c["industry"],
                    "employees_count": c["employees_count"],
                    "employees_bucket": c["employees_bucket"],
                    "founded_year": c["founded_year"],
                    "country": c["country"],
                }
            )

        if not merged:
            raise ValueError(
                "No joined rows on domain; increase quantities or adjust join."
            )

        with open(merged_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=merged[0].keys())
            w.writeheader()
            w.writerows(merged)

        logging.info("Merged %d rows → %s", len(merged), merged_path)
        return merged_path

    # ------------------------------------------------------------------------------
    # LOAD → POSTGRES
    # ------------------------------------------------------------------------------
    @task()
    def load_csv_to_pg(
        conn_id: str,
        csv_path: str,
        schema: str = SCHEMA,
        table: str = TARGET_TABLE,
        append: bool = APPEND,
    ) -> int:
        # Read CSV
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cols = reader.fieldnames or []
            rows = [tuple((r.get(col, "") or None) for col in cols) for r in reader]

        if not rows:
            logging.warning("No rows found in CSV; nothing to insert.")
            return 0

        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join([f'{col} TEXT' for col in cols])}
            );
        """
        maybe_truncate_sql = f"TRUNCATE TABLE {schema}.{table};" if not append else None
        insert_sql = f"""
            INSERT INTO {schema}.{table} ({', '.join(cols)})
            VALUES ({', '.join(['%s' for _ in cols])});
        """

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()

        try:
            with conn.cursor() as cur:
                cur.execute(create_schema_sql)
                cur.execute(create_table_sql)
                if maybe_truncate_sql:
                    cur.execute(maybe_truncate_sql)
                    logging.info("Truncated %s.%s before insert", schema, table)

                cur.executemany(insert_sql, rows)
                conn.commit()

            logging.info("Inserted %d rows into %s.%s", len(rows), schema, table)
            return len(rows)

        except DatabaseError as e:
            conn.rollback()
            logging.exception("Database error during load_csv_to_pg: %s", e)
            return 0
        finally:
            conn.close()

    # ------------------------------------------------------------------------------
    # ANALYZE (reads back from Postgres, creates PNG)
    # ------------------------------------------------------------------------------
    @task()
    def analyze_postgres(conn_id: str, schema: str, table: str, output_dir: str) -> str:
        import pandas as pd
        import matplotlib.pyplot as plt

        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = f"""
            SELECT industry,
                   AVG(NULLIF(employees_count, '')::INT) AS avg_employees
            FROM {schema}.{table}
            GROUP BY industry
            HAVING COUNT(*) >= 3
            ORDER BY avg_employees DESC
            LIMIT 10;
        """
        df = hook.get_pandas_df(sql)

        if df.empty:
            raise ValueError("No data returned for analysis.")

        ax = df.plot(
            kind="bar",
            x="industry",
            y="avg_employees",
            legend=False,
            figsize=(8, 5),
            rot=45,
            title="Top Industries by Average Company Size",
        )
        ax.set_xlabel("Industry")
        ax.set_ylabel("Avg Employees")

        outpath = os.path.join(output_dir, "industry_avg_employees.png")
        plt.tight_layout()
        plt.savefig(outpath, dpi=150)
        logging.info("Saved analysis chart → %s", outpath)
        return outpath

    # ------------------------------------------------------------------------------
    # CLEANUP
    # ------------------------------------------------------------------------------
    @task()
    def clear_folder(folder_path: str = OUTPUT_DIR) -> None:
        if not os.path.exists(folder_path):
            logging.info("Folder %s does not exist; nothing to clear.", folder_path)
            return

        removed = 0
        for name in os.listdir(folder_path):
            path = os.path.join(folder_path, name)
            try:
                if os.path.isfile(path) or os.path.islink(path):
                    os.remove(path)
                    removed += 1
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                    removed += 1
            except Exception as e:
                logging.warning("Failed to delete %s: %s", path, e)

        logging.info("Cleaned %d items from %s", removed, folder_path)

    # ------------------------------------------------------------------------------
    # TASKGROUPS & DEPENDENCIES
    # ------------------------------------------------------------------------------
    out_dir = ensure_output_dir()

    with TaskGroup(group_id="ingest") as ingest:
        persons_file = fetch_persons(out_dir)
        companies_file = fetch_companies(out_dir)

    with TaskGroup(group_id="transform") as transform:
        persons_tx = transform_persons(persons_file)
        companies_tx = transform_companies(companies_file)

    with TaskGroup(group_id="load") as load:
        merged_csv = merge_csvs_tx(persons_tx, companies_tx, out_dir)
        inserted = load_csv_to_pg(
            conn_id=PG_CONN_ID,
            csv_path=merged_csv,
            schema=SCHEMA,
            table=TARGET_TABLE,
            append=APPEND,
        )

    with TaskGroup(group_id="analyze") as analyze:
        chart = analyze_postgres(
            conn_id=PG_CONN_ID,
            schema=SCHEMA,
            table=TARGET_TABLE,
            output_dir=OUTPUT_DIR,
        )

    # Graph: ensure dir → ingest → transform → load → analyze → cleanup
    out_dir >> ingest >> transform >> load >> analyze >> clear_folder(OUTPUT_DIR)
