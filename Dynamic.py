import os
import logging
from sqlalchemy import create_engine, inspect,text
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor

# Configure Logging
logging.basicConfig(
    filename="etl_schema_update.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Database Configurations (from environment variables)
oracle_user = os.getenv("ORACLE_USER")
oracle_password = os.getenv("ORACLE_PASSWORD")
oracle_dsn = os.getenv("ORACLE_DSN")

postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_db = os.getenv("POSTGRES_DB")

# SOURCE_DB = f"oracle+cx_oracle://{oracle_user}:{oracle_password}@{oracle_dsn}"
# TARGET_DB = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
SOURCE_DB = f"oracle+cx_oracle://system:root@localhost/xe"
TARGET_DB  = f"postgresql+psycopg2://postgres:root@localhost:5432/demo1"


# Initialize Engines
source_engine = create_engine(SOURCE_DB)
target_engine = create_engine(TARGET_DB)  
print(source_engine) 
print(target_engine) 


def fetch_table_schema(engine, table_name):
    """Fetch table schema dynamically."""
    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        schema = [col["name"] for col in columns]
        logging.info(f"Fetched schema for table '{table_name}': {schema}")
        return schema
    except Exception as e:
        logging.error(f"Error fetching schema for table '{table_name}': {str(e)}")
        return []


# def sync_and_update_schema(source_table, target_table):
#     """Detect schema drift and update the PostgreSQL table structure dynamically."""
#     try:
#         source_schema = fetch_table_schema(source_engine, source_table)
#         target_schema = fetch_table_schema(target_engine, target_table)

#         # Identify new columns
#         new_columns = [col for col in source_schema if col not in target_schema]

#         if new_columns:
#             logging.info(f"Adding new columns to target table '{target_table}': {new_columns}")
#             with target_engine.connect() as conn:
#                 for col in new_columns:
#                     alter_query = f'ALTER TABLE "{target_table}" ADD COLUMN "{col}" Text;'
#                     conn.execute(alter_query)
#             logging.info(f"Updated schema for table '{target_table}'.")

#     except Exception as e:
#         logging.error(f"Error syncing schema for table '{target_table}': {str(e)}")

def sync_and_update_schema(source_table, target_table):
    """Detect schema drift and update the PostgreSQL table structure dynamically."""
    try:
        source_schema = fetch_table_schema(source_engine, source_table)
        target_schema = fetch_table_schema(target_engine, target_table)

        # Identify new columns
        new_columns = [col for col in source_schema if col not in target_schema]
        # new_columns = [(col, source_schema[col]) for col in source_schema if col not in target_schema]
        print(source_schema)
        print("//////////")
        print(new_columns)

        print(col)


        if new_columns:
            logging.info(f"Adding new columns to target table '{target_table}': {new_columns}")
            with target_engine.connect() as conn:
                for col in new_columns:
                    alter_query = f'ALTER TABLE {target_table} ADD COLUMN {col} text;'
                    conn.execute(text(alter_query))
                    conn.commit()
            logging.info(f"Updated schema for table '{target_table}'.")
        # print("hiiii")
        # ami =conn.execute(f"SELECT count(*) FROM {target_table}")
        # print(ami)

        return new_columns

    except Exception as e:
        logging.error(f"Error syncing schema for table '{target_table}': {str(e)}")
        return []



def truncate_table(table_name):
    """Truncate a PostgreSQL table."""
    try:
        with target_engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            logging.info(f"Truncated table '{table_name}'.")
            
    except Exception as e:
        logging.error(f"Error truncating table '{table_name}': {str(e)}")



def load_data(engine, table_name):
    """Load data from the source table into a DataFrame."""
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        logging.info(f"Loaded {len(df)} rows from source table '{table_name}'.")
        return df
    except Exception as e:
        logging.error(f"Error loading data from table '{table_name}': {str(e)}")
        return pd.DataFrame()


def update_target_table(df, engine, table_name):
    """Update the target table dynamically."""
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False)
        logging.info(f"Inserted {len(df)} rows into target table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error updating target table '{table_name}': {str(e)}")


def etl_process(source_table, target_table):
    try:
        logging.info(f"Starting ETL for source: {source_table}, target: {target_table}")

        # Sync schemas and get new columns
        new_columns = sync_and_update_schema(source_table, target_table)

        # Truncate target table
        truncate_table(target_table)

        # Load source data
        df = load_data(source_engine, source_table)

        # Add new columns to the DataFrame with default values (None)
        for col in new_columns:
            if col not in df.columns:
                df[col] = None

        # Update target table
        update_target_table(df, target_engine, target_table)

        logging.info(f"ETL completed for source: {source_table}, target: {target_table}")

    except Exception as e:
        logging.error(f"ETL failed for source: {source_table}, target: {target_table}. Error: {str(e)}")


def run_etl_for_all_tables():
    """Run ETL for all table mappings."""
    with open("tables.json", "r") as f:
        table_mappings = json.load(f)["table_mappings"]

    # Process each table
    with ThreadPoolExecutor() as executor:
        for mapping in table_mappings:
            executor.submit(
                etl_process, mapping["source_table"], mapping["target_table"]
            )


if __name__ == "__main__":
    run_etl_for_all_tables()
