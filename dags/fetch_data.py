# from datetime import datetime
# from airflow import DAG
# from BoxtoS3Operator import BoxtoS3Operator
#
# default_args = {
#     "owner": "altus-power",
#     "depends_on_past": False,
#     "start_date": datetime(2023, 6, 1),
#     "retries": 1,
# }
#
# with DAG("box_to_s3_dag", default_args=default_args, schedule_interval="0 0 * * *") as dag:
#     box_to_s3_task = BoxtoS3Operator(
#         task_id="box_to_s3_transfer",
#         choice="folder",
#         input_box_folder_id="252788521422",
#         output_s3_bucket="altus-document-testing",
#     )
#
#     box_to_s3_task

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_folder(id, http_conn_id):
    http_hook = HttpHook(http_conn_id=http_conn_id, method='GET')
    response = http_hook.run(f'/2.0/folders/{id}')
    return response.json()

def find_path_collection(folder):
    path_collection = [entry['name'] for entry in folder['path_collection']['entries']]
    path_collection.append(folder['name'])
    return path_collection

def find_items(id, name, http_conn_id, s3_conn_id, s3_bucket_name):
    list_path_file = []
    folder = fetch_folder(id, http_conn_id)
    item_collections = folder['item_collection']
    list_folder = []
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    for entry in item_collections['entries']:
        if entry['type'] == 'file':
            if not list_path_file:
                list_path_file = find_path_collection(folder)
            list_path_file.append(entry['name'])
            file_content = requests.get(entry['url']).content
            full_path = '/'.join(list_path_file)
            s3_hook.load_bytes(file_content, key=full_path, bucket_name=s3_bucket_name, replace=True)
            list_path_file.pop()
        else:
            list_folder.append(entry)

    for entry in list_folder:
        find_items(entry['id'], name, http_conn_id, s3_conn_id, s3_bucket_name)

with DAG(
    'box_to_s3',
    default_args=default_args,
    description='Transfer files from Box to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_box_to_s3',
        python_callable=find_items,
        op_args=['237368240621', 'folder_name', 'http_box', 'aws_default', 'your-s3-bucket-name'],
    )

    transfer_task
