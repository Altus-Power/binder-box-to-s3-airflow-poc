from airflow.hooks.base_hook import BaseHook
from boxsdk import OAuth2, Client
from airflow.hooks.S3_hook import S3Hook
import logging
import json

log = logging.getLogger(__name__)
logging.getLogger("boxsdk").setLevel(logging.CRITICAL)

class BoxHook(BaseHook):
    def __init__(self, *args, **kwargs) -> None:
        self.client = None
        super().__init__(*args, **kwargs)

    def get_conn(self) -> Client:
        # Get Box connection details from Airflow Connection
        client_secret = BaseHook.get_connection("box_conn").get_password()
        client_id = BaseHook.get_connection("box_conn").login
        extra = json.loads(BaseHook.get_connection("box_conn").get_extra())

        oauth = OAuth2(
            client_id=client_id,
            client_secret=client_secret,
            access_token=extra['access_token'],
            refresh_token=extra['refresh_token'],
        )

        client = Client(oauth)
        return client

    def fetch_file(self, client, file_id: str):
        return client.file(file_id).get()

    def find_path_collection(self, item):
        # Assuming this function returns the path collection for the given folder
        path_collection = []
        for entry in item.path_collection['entries']:
            path_collection.append(entry.name)
        path_collection.append(item.name)
        return path_collection

    def download_file_content(self, client, file):
        file_content = client.file(file.id).content()
        return file_content

