import snowflake.connector
import os

# Snowflake connection parameters
SNOWFLAKE_USER = 'zainabrehman'
SNOWFLAKE_PASSWORD = '2F3bUdrmjT2ZJUH'
SNOWFLAKE_ACCOUNT = 'ZVGAAJF-WA82043'  # e.g. xy12345.us-east-1
SNOWFLAKE_WAREHOUSE = 'university_warehouse'
SNOWFLAKE_DATABASE = 'UNIVERSITY_DWH'
SNOWFLAKE_SCHEMA = '"university_dwh.public"'

TRANSFORMED_DIR = "/home/zainab/university_dwh_project/data/transformed"

def create_connection():
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return ctx

def create_file_format(cursor):
    cursor.execute("""
        CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','   
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', 'null', '')
    """)

def load_table_from_csv(table_name, csv_file_path, cursor):
    print(f"Uploading {csv_file_path} to table {table_name}...")

    # Upload local file to internal stage for the table
    put_command = f"PUT file://{csv_file_path} @%{table_name} OVERWRITE = TRUE"
    cursor.execute(put_command)

    # Load data from internal stage to table
    copy_command = f"""
        COPY INTO {table_name}
        FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
        ON_ERROR = 'CONTINUE'
    """
    cursor.execute(copy_command)
    print(f"Loaded data into {table_name} successfully.")

def main():
    ctx = create_connection()
    cs = ctx.cursor()
    try:
        cs.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cs.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        create_file_format(cs)

        tables_and_files = {
            "dim_students": "dim_students.csv",
            "dim_courses": "dim_courses.csv",
            "dim_faculty": "dim_faculty.csv",
            "dim_semesters": "dim_semesters.csv",
            "dim_date": "dim_date.csv",
            "fact_academic_engagement": "fact_academic_engagement.csv"
        }

        for table, file in tables_and_files.items():
            csv_path = os.path.join(TRANSFORMED_DIR, file)
            load_table_from_csv(table, csv_path, cs)

    finally:
        cs.close()
        ctx.close()

if __name__ == "__main__":
    main()
