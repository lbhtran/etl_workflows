import pandas as pd

from prefect import task
from prefect import Flow
from prefect import Parameter

from books_prefect_wf.api_requests import NytBooksApis


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
def load_book_details_dim(df: pd.DataFrame):
    cols = ['title', 'author', 'publisher', 'description', 'primary_isbn13', 'primary_isbn10']