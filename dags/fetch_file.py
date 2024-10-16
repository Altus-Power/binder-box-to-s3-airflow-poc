# from datetime import datetime
# from airflow import DAG
# from BoxtoS3Operator import BoxtoS3Operator
# from airflow.decorators import task
#
# default_args = {
#     "owner": "altus-power",
#     # "depends_on_past": False,
#     # "start_date": datetime(2023, 6, 1),
#     # "retries": 1,
# }
#
# class Box2S3Dag:
#     def __init__(self):
#         self.dag_id = "box-to-s3-dag"
#         self.dag = self._create_dag()
#
#     def _create_dag(self):
#         return DAG(
#             dag_id=self.dag_id,
#             display_name="Box2S3",
#             catch_up=False,
#             start_date=datetime.now(),
#             schedule_interval=None,
#             default_args=default_args
#
#         )
#
#     def build_dag(self):
#         with self.dag:
#             @task
#             def fetch_data(**kwargs):
#                 pass
#
#
#         return self.dag
#
# obj = Box2S3Dag()
# globals()[obj.dag_id] = obj.build_dag()
#
