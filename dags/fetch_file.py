from datetime import datetime
from airflow import DAG
from BoxtoS3Operator import BoxtoS3Operator

default_args = {
    "owner": "altus-power",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "retries": 1,
}

with DAG("box_to_s3_dag", default_args=default_args, schedule_interval=None) as dag:
    box_to_s3_task = BoxtoS3Operator(
        task_id="box_to_s3_transfer",
        input_box_file_id="1669641337704",
        output_s3_bucket="altus-document-testing",
    )

    box_to_s3_task
