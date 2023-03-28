CREATE FULLTEXT INDEX CustomerIndex1 FOR (n:Customer1) ON EACH [n.name1];
CREATE FULLTEXT INDEX CustomerIndex21 FOR (n:Customer21) ON EACH [n.name12];
CREATE FULLTEXT INDEX CustomerIndex231 FOR (n:Customer213) ON EACH [n.name123];
CREATE INDEX node_index_name FOR (n:Person) ON (n.surname);
