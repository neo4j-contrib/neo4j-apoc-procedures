MATCH (n:Node {id:1}) RETURN n;
MATCH (start:Start {id:1})-[rel:REL {id: 2}]->(end:End {id: 3}) RETURN start, rel, end;
MATCH path=(start:StartPath)-[:REL_PATH]->(:EndPath) RETURN path ORDER BY start.id;
MATCH path=(start:StartPath)-[:REL_PATH]->(:EndPath) WITH path ORDER BY start.id RETURN collect(path) as paths;