import pandas as pd

from prefect import task
from prefect import Flow
from prefect import Parameter

from books_prefect_wf.api_requests import NytBooksApis
from books_prefect_wf.database import Database
from books_prefect_wf.database import BookDetailsDim

book_details_cols = ['title', 'author', 'publisher', 'description', 'primary_isbn13', 'primary_isbn10']

@task
def extract_book_listnames():
    session = NytBooksApis()
    listnames = session.get_list_names()
    return [list_name['list_name_encoded'] for list_name in listnames['results']]

@task
def extract_book_details(listnames: list):
    session = NytBooksApis()
    all_dfs = []
    for listname in listnames:
        books = session.get_books(listname)
        book_details = [book['book_details'][0] for book in books['results']]
        df_temp = pd.DataFrame(book_details)
        all_dfs.append(df_temp)
    return pd.concat(all_dfs).drop_duplicates()

@task
def extract_book_details_dim():
    db = Database(conn_id='books_db')
    df = db.query_table_to_df(BookDetailsDim)
    df = df.fillna('')
    return df

@task
def transform_book_details_delta(df_books_api, df_books_db):
    df = pd.concat([df_books_api[book_details_cols], df_books_db[book_details_cols]], ignore_index=True)
    df = df.drop_duplicates(keep=False)
    return df


@task
def load_book_details_dim(df: pd.DataFrame):
    db = Database(conn_id='books_db')
    db.add_book_details_data(df.to_dict('records'), book_details_cols)


def main():
    with Flow("etl") as flow:

        df_book_details_db = extract_book_details_dim()
        listnames = extract_book_listnames()
        df_book_details_api = extract_book_details(listnames[7:12])

        df_book_details = transform_book_details_delta(df_book_details_api, df_book_details_db)

        load_book_details_dim(df_book_details)

    flow.run()


if __name__ == "__main__":
    main()