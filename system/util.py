import json
from typing import Any

class URL():
    def __init__(self, url, depth) -> None:
        self.url = url
        self.depth = depth
    
    def __str__(self) -> str:
        return f'URL({self.url}, {self.depth})'

    def __repr__(self) -> str:
        return str(self)

    def get_url(self) -> str:
        return self.url

class URLEncoder(json.JSONEncoder):
    def default(self, o: URL) -> Any:
        if isinstance(o, URL):
            return {'url': [o.url, o.depth]}

def as_url(dct):
    if 'url' in dct:
        return URL(dct['url'][0], dct['url'][1])
    return dct


# url = URL('https://pavpanchekha.com/', 2)
# string = json.dumps(url, cls=URLEncoder)
# print(string)
# print(json.loads(string, object_hook=as_url))