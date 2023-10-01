from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'me',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def sample_etl_pipeline():
    @task()
    def extract_data():
        """
        Extraction part of ETL pipeline
        We can extract data from many sources such as S3, MYSQL, postgres,
        DynamoDB, etc.
        """
        return True

    @task()
    def transform_data(extracted_data_path):
        """
        This is the transformation part of any pipeline
        We can apply any transformation to the data retrieved by extraction
        """
        return True

    @task()
    def load_data(transformed_data_path):
        """
        Load data.
        We can load the data to any database as per your requirements
        """
        return True

        extracted_data_path = extract_data()
        transformed_data = transform_data(extracted_data_path)
        load_data(transformed_data)


dag_run = sample_etl_pipeline()
