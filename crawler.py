from bs4 import BeautifulSoup
import redis, sys
from redisgraph import Graph
from url_normalize import url_normalize as normalize
import requests
from pprint import pprint

def get_url_list(url):
    try:
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        links = soup.find_all('a')

        links = filter(lambda link: 'href' in link.attrs and link['href'].startswith('https'), links)
        links = map(lambda link: normalize(link['href']), links)
        return links
    except Exception as e:
        print(e)
        return []

def create_edge(url1, url2, graph: Graph):
    # print("Command: ", f"MATCH (n:node) WHERE n.url = '{url1}' CREATE (n)-[:href]->(:node {{url:'{url2}'}})")
    graph.query(f"MATCH (n:node) WHERE n.url = '{url1}' CREATE (n)-[:href]->(:node {{url:'{url2}'}})")# .pretty_print()

def crawler(url, depth):
    url = normalize(url)
    pages_crawled = {url}
    q = [url]
    l = len(q)
    current_depth = 0

    r = redis.Redis(host='localhost', port=6379)
    graph = Graph("urltest", r)

    #print("Command: ", f"CREATE (:node {{url:'{url}'}})")
    graph.query(f"CREATE (:node {{url:'{url}'}})").pretty_print()

    while q and current_depth != depth:
        count = len(q)
        while count > 0:
            url = q.pop(0)
            links = get_url_list(url)
            links = filter(lambda link: link not in pages_crawled, links)
            
            for link in links:
                create_edge(url, link, graph)
                pages_crawled.add(link)
                q.append(link)
            count -= 1
        current_depth += 1
    
    return pages_crawled

s = crawler(sys.argv[1], int(sys.argv[2]))
pprint(s)
print(len(s))
