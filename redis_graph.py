import redis
from redisgraph import Node, Edge, Graph
from URLGraph import URLGraph

r = redis.Redis(host='localhost', port=6379)

graph = Graph('test2', r)

# graph.query("CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}), (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}), (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})").pretty_print()

graph.query("MATCH (a:Rider), (b:Team) WHERE a.name = 'Valentino Rossi' AND b.name = 'Yamaha' CREATE (a)-[:rides]->(b)").pretty_print()
