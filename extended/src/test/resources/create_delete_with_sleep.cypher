CREATE (n:Node {id:1});

CALL apoc.util.sleep(2000)

MATCH (n)
DELETE n;
