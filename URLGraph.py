import redis
from redisgraph import Node, Edge, Graph

class URLGraph:
    def __init__(self, name, redis_db) -> None:
        self.graph = Graph(name, redis_db)
    
    def add_node(self, url: str) -> Node:
        url_node = Node(label='node', properties={'url' : url})
        self.graph.add_node(url_node)
        return url_node

    def add_edge(self, nodeA: Node, nodeB: Node) -> None:
        self.graph.add_edge(Edge(nodeA, 'href', nodeB))
    
    def commit(self) -> None:
        self.graph.commit()
