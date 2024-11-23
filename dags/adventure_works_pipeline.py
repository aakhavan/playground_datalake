
import yaml
import boto3
import csv
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    's3_to_rds_postgres',
    default_args=default_args,
    description='A DAG to load CSV files from S3 to RDS PostgreSQL',
    schedule_interval=None, # for now being run manually
)


def load_table_schemas():
    """Load the table schemas from the YAML file"""
    with open('config/tables_schema.yaml', 'r') as file:
        return yaml.safe_load(file)

def parse_csv_to_generator(filepath):
    """Parse the CSV file and return a generator"""
    import csv

    with open(filepath, 'r') as file:
        yield from csv.reader(file, delimiter='\t')


def parse_csv_skip_header(filepath):
    import csv
    with open(filepath, newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Skip the header row
        return list(reader)

def generate_sql_statements(table_name, schema, column_list):

    # Generate the SQL to recreate the table
    columns_sql = ", ".join([f"{col} VARCHAR" for col in column_list])
    sql = f"""
    DROP TABLE IF EXISTS {schema}.{table_name};
    CREATE TABLE {schema}.{table_name} (
        {columns_sql}
    );
    """

    return sql

# dummy tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)


table_config = load_table_schemas()

for table in table_config['tables']:
    task_id = f"load_{table['name']}_to_postgres"
    s3_key = f"adventure_works_data/{table['s3_name']}.csv"
    table_name = table['name']
    schema =table_config['schema']
    table_full_name = f"{schema}.{table['name']}"
    column_list = table['fields']

    # Task to drop and create the table
    drop_create_table_task = PostgresOperator(
        task_id=f"drop_create_{table_name}_table",
        postgres_conn_id='postgres_conn',
        sql=generate_sql_statements(table_name, schema, column_list),
        params={"schema": schema},
        dag=dag,
    )

    load_csv_to_postgres = S3ToSqlOperator(
        task_id=task_id,
        s3_bucket='soufiane-amir-bucket',
        s3_key=s3_key,
        table=table_full_name,
        parser=parse_csv_skip_header,
        column_list=column_list,
        sql_conn_id='postgres_conn',
        aws_conn_id='s3_connection',
        dag=dag,
    )

    start_task >> drop_create_table_task >> load_csv_to_postgres >> end_task