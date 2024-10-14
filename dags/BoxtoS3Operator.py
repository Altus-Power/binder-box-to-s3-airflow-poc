import logging
import os
import tempfile
from datetime import datetime
from typing import Optional

import pendulum
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, Variable
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from boxsdk import Client
from BoxHook import BoxHook

est_tz = pendulum.timezone("EST5EDT")
log = logging.getLogger(__name__)
logging.getLogger("boxsdk").setLevel(logging.CRITICAL)

S3_HISTORY = f"default_bucket"


class BoxtoS3Operator(BaseOperator):
    """
    Downloads files/folders from Box and uploads them to S3.

    :param choice: The choice between 'file' or 'folder'.
    :type choice: str
    :param input_box_folder_id: The folder ID of the Box location.
    :type input_box_folder_id: str, optional
    :param input_box_file_id: The file ID of the Box location.
    :type input_box_file_id: str, optional
    :param output_s3_key: The S3 key to store the downloaded data.
    :type output_s3_key: str, optional
    :param output_s3_bucket: The S3 bucket to store the downloaded data.
    :type output_s3_bucket: str, optional
    """

    ui_color = "#c8f1eb"

    @apply_defaults
    def __init__(
        self,
        choice: str,
        input_box_folder_id: Optional[str] = None,
        input_box_file_id: Optional[str] = None,
        output_s3_key: Optional[str] = None,
        output_s3_bucket: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.choice = choice
        self.input_box_folder_id = input_box_folder_id
        self.input_box_file_id = input_box_file_id
        self.output_s3_key = output_s3_key
        self.output_s3_bucket = output_s3_bucket or S3_HISTORY

    def __generate_dttm(self) -> str:
        """Generate a timestamp string in the EST timezone."""
        return datetime.now(est_tz).strftime("%Y%m%d%H%M")

    def __box_to_local(self, box_client: Client, file_name: str, file_id: str) -> None:
        """Download a file from Box and save it locally."""
        path = os.getcwd()
        file_path = os.path.join(path, file_name)
        log.info("File path is {}".format(file_path))
        with open(file_path, "wb") as open_file:
            box_client.file(file_id).download_to(open_file)

    def __local_to_s3(self, s3_client: S3Hook, dttm: str, file_name: str) -> None:
        """Upload a local file to S3."""
        s3_key = "{}/dttm={}/{}".format(self.output_s3_key, dttm, file_name)
        s3_client.load_file(
            filename=file_name, key=s3_key, bucket_name=self.output_s3_bucket
        )
        log.info(f"{file_name} copied to {s3_key}")

    def __validate_parameters(self) -> Optional[str]:
        """Validate the operator's parameters."""
        if self.choice not in ["file", "folder"]:
            return "Invalid choice. Select between 'file' or 'folder'."
        elif self.choice == "folder" and not self.input_box_folder_id:
            return "Folder ID is not provided."
        elif self.choice == "file" and not self.input_box_file_id:
            return "File ID is not provided."
        return None

    def execute(self, context):
        err = self.__validate_parameters()
        if err:
            raise AirflowException(err)

        bh = BoxHook()
        client = bh.get_conn()
        user = client.user().get()
        log.info("Current User is {} and User ID is {}".format(user.name, user.id))
        dttm = self.__generate_dttm()
        log.info("Generated Dttm: {}".format(dttm))

        with tempfile.TemporaryDirectory() as tmp_dir:
            os.chdir(tmp_dir)
            log.info("Current working temp directory: {}".format(tmp_dir))

            items = (
                bh.get_folder_items(self.input_box_folder_id)
                if self.choice == "folder"
                else [client.file(self.input_box_file_id).get()]
            )
            client_hook = S3Hook("s3_conn")
            for item in items:
                log.info(
                    '{0} {1} is named "{2}"'.format(
                        item.type.capitalize(), item.id, item.name
                    )
                )

                self.__box_to_local(client, item.name, item.id)
                self.__local_to_s3(client_hook, dttm, item.name)

                os.remove(item.name)


class ETLBoxtoS3Operator(AirflowPlugin):
    name = "BoxtoS3Operator"
    operators = [BoxtoS3Operator]