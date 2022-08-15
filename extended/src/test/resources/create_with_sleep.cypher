UNWIND RANGE(0,2) as id
CREATE (n:Node {id:id})
RETURN n.id as id;

CALL apoc.util.sleep(1000)

MATCH (n) DELETE n;
