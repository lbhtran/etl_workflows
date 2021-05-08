import logging

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from hbs_plugin.hooks.nyt_hooks import NytBooksApiHook


class NytBooksApiOperator(BaseOperator):
    """
    This operator gets data from NYT Books API and load to book_details_dim table
    """

    @apply_defaults
    def __init__(
            self,
            src_conn_id,
            dest_conn_id,
            params=None,
            *args,  **kwargs):
        super(NytBooksApiOperator, self).__init__(*args, **kwargs)
        self.src_conn_id = src_conn_id
        self.dest_conn_id = dest_conn_id
        self.params = params

    def execute(self, context):
        """
        Create a new customer in the CRM system.
        """
        logging.info('Getting names of NYT Bestseller lists')
        # get data from api
        # transformation
        # load data into table