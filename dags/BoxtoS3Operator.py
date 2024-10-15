import logging
from datetime import datetime
from typing import Optional

import pendulum
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from Hook import BoxHook

est_tz = pendulum.timezone("EST5EDT")
log = logging.getLogger(__name__)
logging.getLogger("boxsdk").setLevel(logging.CRITICAL)

S3_HISTORY = "default_bucket"

class BoxtoS3Operator(BaseOperator):
    ui_color = "#c8f1eb"

    @apply_defaults
    def __init__(
            self,
            input_box_file_id: Optional[str] = None,
            output_s3_bucket: Optional[str] = None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.input_box_file_id = input_box_file_id
        self.output_s3_bucket = output_s3_bucket

    def __generate_dttm(self) -> str:
        """Generate a timestamp string in the EST timezone."""
        return datetime.now(est_tz).strftime("%Y%m%d%H%M")

    def save_file_to_s3(self, list_path_file, file_content):
        s3_client = S3Hook("s3_conn")
        full_path = '/'.join(list_path_file)
        s3_client.put_object(Bucket=self.output_s3_bucket, Key=full_path, Body=file_content)

    def execute(self, context):
        bh = BoxHook()
        client = bh.get_conn()
        user = client.user().get()
        log.info("Current User is {} and User ID is {}".format(user.name, user.id))
        dttm = self.__generate_dttm()
        log.info("Generated Dttm: {}".format(dttm))

        file = bh.fetch_file(client, self.input_box_file_id)
        list_path_item = bh.find_path_collection(file)
        content = bh.download_file_content(client, file)
        self.save_file_to_s3(list_path_item, content)
