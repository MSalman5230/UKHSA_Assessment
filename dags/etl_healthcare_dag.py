from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras

CSV_PATH = (
    "healthcare_dataset_.csv"  # for cloud premise this would be path to S3 storage,
)

# Define the default arguments for the DAG
default_args = {
    "owner": "masood",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}
# Define the DAG object
dag = DAG(
    "ETL_Healthcare",
    default_args=default_args,
    description="Load Heathcare CSV to Postgres DB",
    schedule_interval="@daily",  # Run the DAG daily
    catchup=False,
)


# Helper functions
def get_postgres_connection():
    # Set up connection parameters
    host = "192.168.11.3"  # os.environ['PG_HOST']
    database = "aaaaa"  # os.environ['PG_DB']
    user = "aaaaa"  # os.environ['PG_USER']
    password = "----"  # os.environ['PG_PASS']

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host, database=database, user=user, password=password
        )
        print("Postgres connected")
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        raise error


def postgres_insert(conn, df, table):
    """
    funtion to insert df to postgresdb
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ",".join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(
            cursor, query, tuples
        )  # this method is used because of performance https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        raise error
        # return 1
    print("execute_values() done")
    cursor.close()


# Define the function to be executed by the PythonOperator
def run_etl():
    df = pd.read_csv(
        CSV_PATH, index_col=False
    )  # for cloud premise this would be path to S3 storage

    df.drop_duplicates(inplace=True)
    df["Date of Admission"] = (
        pd.to_datetime(df["Date of Admission"], format="%d/%m/%y", errors="coerce")
        .fillna(
            pd.to_datetime(df["Date of Admission"], format="%d.%m.%y", errors="coerce")
        )
        .fillna(
            pd.to_datetime(df["Date of Admission"], format="%d-%m-%y", errors="coerce")
        )
    )

    df["Discharge Date"] = (
        pd.to_datetime(df["Discharge Date"], format="%d/%m/%y", errors="coerce")
        .fillna(
            pd.to_datetime(df["Discharge Date"], format="%d.%m.%y", errors="coerce")
        )
        .fillna(
            pd.to_datetime(df["Discharge Date"], format="%d-%m-%y", errors="coerce")
        )
    )

    df.drop(columns=["Name"], inplace=True)

    for index, row in df.iterrows():
        # Extract numerical value from the end of the Insurance Provider
        numerical_value = row["Insurance Provider"].split()[-1]

        # Check if number extracted is numerical
        if numerical_value.replace(".", "").isnumeric():
            df.at[index, "Billing Amount"] = float(numerical_value)
            df.at[index, "Insurance Provider"] = row["Insurance Provider"].replace(
                numerical_value, ""
            )

    df["Stay Length days"] = (df["Discharge Date"] - df["Date of Admission"]).dt.days
    # Here we can drop the 'Discharge Date' as we can calculate Discharge Date by using Date of Admission + Stay Length
    df.drop(columns=["Discharge Date"], inplace=True)
    # Rename columns to lowercase and replace spaces with underscores, for DB Entry
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Validate Data
    nan_values = (
        df[
            [
                "age",
                "gender",
                "date_of_admission",
                "doctor",
                "hospital",
                "billing_amount",
                "admission_type",
            ]
        ]
        .isnull()
        .sum()
    )
    if nan_values.sum() > 0:
        raise Exception(f"Missing Key Data in {CSV_PATH}")

    # raise Exception("Sorry, no numbers below zero")


# Define the task using PythonOperator
task_ETL_HealthCare = PythonOperator(task_id="ETL_HealthCare", python_callable=run_etl, dag=dag)
