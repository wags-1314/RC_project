import pika, time, sys, os, json, util
from url_normalize import url_normalize as normalize
from pprint import pprint
import redis
from redisgraph import Graph
from pottery import Redlock

urls_seen = set()

def main():
    r = redis.Redis(host='localhost', port=6379)

    url = normalize(sys.argv[1])
    depth = int(sys.argv[2])
    urls_seen.add(url)

    if r.exists("urls_seen"):
        r.delete("urls_seen")
    
    if r.exists("url_graph"):
        r.delete("url_graph")

    r.sadd("urls_seen", url)

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    
    graph = Graph("url_graph", r)
    graph.query(f"CREATE (:node {{url:'{url}'}})")

    def callback(ch, method, properties, body):
        print(f' [x] Received {body}')
        links = json.loads(body, object_hook=util.as_url)
        for url_obj in links:
            #if url_obj.url not in urls_seen:

            if url_obj.depth < depth:
                message = json.dumps(url_obj, cls=util.URLEncoder)
                print(f' [x] Sending {message} back.')
                channel.basic_publish(exchange='',
                                    routing_key='url',
                                    body=message)

    channel.queue_declare(queue='url')
    channel.queue_declare(queue='res')
    channel.queue_purge(queue='url')
    channel.queue_purge(queue='res')

    message = json.dumps(util.URL(url, 0), cls=util.URLEncoder)

    channel.basic_publish(exchange='', routing_key='url', body=message)
    print(f" [x] Sent {message} to worker queue")
    print(" [*] Waiting for results from worker")

    channel.basic_consume(queue='res', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        print('final set: ')
        pprint(urls_seen)
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)