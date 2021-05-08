import json, os

from typing import List, Any, Dict

from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pandas as pd

Base = declarative_base()

class BookDetailsDim(Base):
    __tablename__ = "book_details_dim"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    author = Column(String)
    publisher = Column(String)
    description = Column(String)
    primary_isbn13 = Column(String(13))
    primary_isbn10 = Column(String(10))
    added_on = Column(DateTime(timezone=True), server_default=func.now())


class Database:

    @staticmethod
    def get_conns(conn_id):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'connections.json')) as f:
            conns = json.load(f)
        return conns[conn_id]

    def __init__(self, conn_id) -> None:
        conn = self.get_conns(conn_id)
        conn_config = {
            "usr": conn['login'],
            "pw": conn['password'] or '',
            "host": conn['host'] or 'localhost',
            "db": conn['schema'] or ''
        }
        self.engine = create_engine(
            f"mysql+pymysql://{conn_config['usr']}:{conn_config['pw']}@{conn_config['host']}/{conn_config['db']}"
        )

        session_maker = sessionmaker()
        session_maker.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)
        self.session = session_maker()

    def query_table_to_df(self, table_object):
        query = self.session.query(table_object)
        df = pd.read_sql(query.statement, self.session.bind)
        return df

    def add_book_details_data(self, vectors: List[Dict[str, Any]], columns) -> None:
        for entry in vectors:
            vec = BookDetailsDim(
                **{k: entry[k] for k in columns}
            )
            # insert or update vector
            self.session.merge(vec)
        self.session.commit()


if __name__ == "__main__":

    db = Database(conn_id='books_db')
    df = db.query_table_to_df(BookDetailsDim)
    print(df.head())
