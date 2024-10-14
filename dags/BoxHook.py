from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
# from boxsdk import CCGAuth, Client
from boxsdk import Client, OAuth2
# from boxsdk.network.default_network import DefaultNetwork
# import requests
import certifi
import ssl
from boxsdk.session.session import Session
import json
import logging
from boxsdk.object.item import Item
from typing import Iterable

log = logging.getLogger(__name__)
logging.getLogger("boxsdk").setLevel(logging.CRITICAL)

class BoxHook(BaseHook):
    """
    Wrap around the Box Python SDK
    """

    def __init__(self, *args, **kwargs) -> None:
        self.client = None
        super().__init__(*args, **kwargs)

    def get_conn(self) -> Client:
        # Get Box connection details from Airflow Connection
        client_secret = BaseHook.get_connection("box_conn").get_password()
        client_id = BaseHook.get_connection("box_conn").login
        extra = json.loads(BaseHook.get_connection("box_conn").get_extra())

        # Authenticate and create a Box client
        # auth = CCGAuth(
        #     client_id=client_id,
        #     client_secret=client_secret,
        #     user=extra["user"]

        # )

        # import requests
        #
        # url = "https://api.box.com/2.0/folders/170674182181?limit=1000"
        #
        # payload = {}
        # headers = {
        #     'Authorization': 'Bearer huTAiFDS6xF3zbSvqQqY4PPLwTjydDQB',
        #     'Cookie': 'box_visitor_id=66f52675ed6306.69303985; site_preference=desktop; uid=36777589243'
        # }
        #
        # response = requests.request("GET", url, headers=headers, data=payload,verify=False)
        #
        # print(response.text)

        oauth = OAuth2(
            client_id=client_id,
            client_secret=client_secret,
            access_token=extra['access_token'],
            refresh_token=extra['refresh_token'],
        )

        # session = requests.Session()
        # session.verify = False
        # client = Client(oauth, network_layer=CustomNetwork(session))
        # custom_network = CustomNetwork(session)
        # client = Client(oauth, session=AuthorizedSession(oauth, network_layer=custom_network))
        client = Client(oauth)
        # client.session.verify = False
        user = client.user().get()
        log.info("Current User is {} and User Id is {}".format(user.name, user.id))
        return client

    def download_file(self, file_id: str, file_name: str) -> None:
        """
        Download a file from Box given its ID and save it with the provided file name.
        """
        client = self.get_conn()
        client.file(file_id=file_id).download_to(file_name)

    def get_folder_items(self, folder_id: str) -> Iterable[Item]:
        """
        Retrieve a list of items within a Box folder given its ID.
        """
        client = self.get_conn()
        root_folder = client.folder(folder_id=folder_id).get()
        items = root_folder.get_items()
        return items