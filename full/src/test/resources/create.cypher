UNWIND RANGE(0,2) as id
CREATE (n:Node {id:id})
RETURN n.id as id;

MATCH (n) DELETE n;
