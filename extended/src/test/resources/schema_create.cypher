:begin
CREATE INDEX FOR (n:Node) ON (n.id);

CREATE (n:Node {id:0});
:commit
