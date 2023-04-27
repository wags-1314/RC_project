from bs4 import BeautifulSoup
import redis
from redisgraph import Graph
from url_normalize import url_normalize as normalize
import requests

def crawler(url, depth):
    url = normalize(url)
    pages_crawled = {url}
    q = [url]
    l = len(q)
    current_depth = 0

    r = redis.Redis(host='localhost', port=6379)
    graph = Graph("urltest", r)

    print("Command: ", f"CREATE (:node {{url:'{url}'}})")
    graph.query(f"CREATE (:node {{url:'{url}'}})").pretty_print()

    while q and current_depth != depth:
        count = len(q)
        while count > 0:
            url = q.pop(0)

            page = requests.get(url)
            soup = BeautifulSoup(page.text, 'html.parser')
            links = soup.find_all('a')

            links = filter(lambda link: 'href' in link.attrs and link['href'].startswith('https'), links)
            links = map(lambda link: normalize(link['href']), links)
            links = filter(lambda link: link not in pages_crawled, links)
            
            for link in links:
                print("Command: ", f"MATCH (n:node) WHERE n.url = '{url}' CREATE (n)-[:href]->(:node {{url:'{link}'}})")
                graph.query(f"MATCH (n:node) WHERE n.url = '{url}' CREATE (n)-[:href]->(:node {{url:'{link}'}})").pretty_print()
                pages_crawled.add(link)
                q.append(link)
            count -= 1
        current_depth += 1
    
    return pages_crawled

print(crawler('https://pavpanchekha.com/', 2))
