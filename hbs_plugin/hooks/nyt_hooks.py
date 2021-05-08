from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import requests
import json, os


class NytBooksApiHook(BaseHook):
    """
    Hook to interact with the NYT Books API
    """

    @staticmethod
    def get_conns(conn_id):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'connections.json')) as f:
            conns = json.load(f)
        return conns[conn_id]

    def __init__(self, api_conn_id='nyt_books_api', *args, **kwargs):
        self.api_token = None
        conn = self.get_conns(api_conn_id)
        self.base_url = conn['host'] if conn['host'] else 'https://api.nytimes.com/svc/books/v3'
        if conn['extra']['API_KEY']:
            self.api_token = conn['extra']['API_KEY']
        self.header = {
            "Accept": "application/json"
        }
        super(NytBooksApiHook, self).__init__(*args, **kwargs)

        # self.api_token = None
        # conn_id = self.get_connection(api_conn_id)
        # self.base_url = conn_id.host if conn_id.host else 'https://api.nytimes.com/svc/books/v3'
        # if conn_id.extra_dejson.get('API_KEY'):
        #     self.api_token = conn_id.extra_dejson.get('API_KEY')
        # self.header = {
        #     "Accept": "application/json"
        # }
        # super().__init__(method='GET', http_conn_id=api_conn_id)

        # self.base_url = 'https://api.nytimes.com/svc/books/v3'
        # self.api_token = api_token
        # self.header = {
        #     "Accept": "application/json"
        # }
        # super().__init__()

    def get_list_names(self):
        requestUrl = f"{self.base_url}/lists/names.json?api-key={self.api_token}"

        response = requests.get(requestUrl, headers=self.header)

        return response.json()

    def get_books(self, book_type):
        requestUrl = f"{self.base_url}/lists.json?list={book_type}&api-key={self.api_token}"

        response = requests.get(requestUrl, headers=self.header)

        return response.json()


if __name__ == '__main__':

    hook = NytBooksApiHook()
    # print(hook.base_url)
    print(hook.get_books('hardcover-fiction')['results'][0])
    print(hook.get_list_names()['results'][0])
