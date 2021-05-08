from airflow.hooks.base import BaseHook

from sqlalchemy import create_engine
import pandas as pd
import json, os


class MysqlHook(BaseHook):
    """
    Hook to interact with the NYT Books API
    """

    @staticmethod
    def get_conns(conn_id):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'connections.json')) as f:
            conns = json.load(f)
        return conns[conn_id]

    def __init__(self, mysql_conn_id='mysql_books', *args, **kwargs):
        super(MysqlHook, self).__init__(*args, **kwargs)
        conn = self.get_conns(mysql_conn_id)
        conn_config = {
            "usr": conn['login'],
            "pw": conn['password'] or '',
            "host": conn['host'] or 'localhost',
            "db": conn['schema'] or ''
        }
        self.engine = create_engine(
            f"mysql+pymysql://{conn_config['usr']}:{conn_config['pw']}@{conn_config['host']}/{conn_config['db']}"
        )

    def db_to_df(self, table_name, columns=None):
        return pd.read_sql_table(table_name, con=self.engine, columns=columns)

    def df_to_db(self, df, table_name, **kwargs):
        df.to_sql(table_name, self.engine, **kwargs)


if __name__ == '__main__':
    hook = MysqlHook()
    df = hook.db_to_df('book_details_dim')
    print(df.shape)