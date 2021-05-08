# Demo code sample. Not indended for production use.

# See instructions for installing Requests module for Python
# http://docs.python-requests.org/en/master/user/install/

import requests

def execute():
  requestUrl = "https://api.nytimes.com/svc/books/v3/lists.json?list=hardcover-fiction&api-key=kLDEMQ3GDZBXPuQn7rb6KVUMQANVubCE"
  requestHeaders = {
    "Accept": "application/json"
  }

  request = requests.get(requestUrl, headers=requestHeaders)

  print(request.content)

if __name__ == "__main__":
  execute()
