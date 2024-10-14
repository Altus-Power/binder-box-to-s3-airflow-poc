from datetime import datetime
from airflow import DAG
from BoxtoS3Operator import BoxtoS3Operator

default_args = {
    "owner": "altus-power",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "retries": 1,
}

with DAG("box_to_s3_dag", default_args=default_args, schedule_interval="0 0 * * *") as dag:
    box_to_s3_task = BoxtoS3Operator(
        task_id="box_to_s3_transfer",
        choice="folder",
        input_box_folder_id="252788521422",
        output_s3_bucket="altus-document-testing",
    )

    box_to_s3_task