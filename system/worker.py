import pika, time, sys, os, requests, json, util
from pottery import Redlock
from url_normalize import url_normalize as normalize
from bs4 import BeautifulSoup
from itertools import islice
import redis
from redisgraph import Graph

def get_links(url):
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

def create_edge(url1: str, url2: str, graph: Graph):
    print("Command: ", f"MATCH (n:node) WHERE n.url = '{url1}' CREATE (n)-[:href]->(:node {{url:'{url2}'}})")
    graph.query(f"MATCH (n:node) WHERE n.url = '{url1}' CREATE (n)-[:href]->(:node {{url:'{url2}'}})")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    r = redis.Redis(host='localhost', port=6379)
    graph = Graph("url_graph", r)

    def callback(ch, method, properties, url):
        print(f' [x] Received {url}')
        url_obj: util.URL = json.loads(url, object_hook=util.as_url)
        #time.sleep(10)
        print(' [*] Getting links from the url...')
        links = get_links(url_obj.url)
        new_links = []

        #print(f'Delaying...')
        #time.sleep(10)

        set_lock = Redlock(key='set_lock', masters={r})

        for link in links:
            if set_lock.acquire():
                if not r.sismember("urls_seen", link):
                    print('Not in', link)
                    try:
                        create_edge(url_obj.url, link, graph)
                        new_links.append(util.URL(link, url_obj.depth + 1))
                    except:
                        pass
                    r.sadd("urls_seen", link)
                set_lock.release()

        if len(new_links) != 0:
            message = json.dumps(new_links, cls=util.URLEncoder)
            print(f' [x] Sending {message} back.')
            channel.basic_publish(exchange='',
                                  routing_key='res',
                                  body=message)
        
        channel.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='url',
                          on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)