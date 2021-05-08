import requests


API_KEY = 'kLDEMQ3GDZBXPuQn7rb6KVUMQANVubCE'


def get_list_names():
    requestUrl = f"https://api.nytimes.com/svc/books/v3/lists/names.json?api-key={API_KEY}"
    requestHeaders = {
        "Accept": "application/json"
    }

    response = requests.get(requestUrl, headers=requestHeaders)

    return response.json()


def get_books(book_type='hardcover-fiction'):
    requestUrl = f"https://api.nytimes.com/svc/books/v3/lists.json?list={book_type}&api-key={API_KEY}"
    requestHeaders = {
        "Accept": "application/json"
    }

    response = requests.get(requestUrl, headers=requestHeaders)

    return response.json()