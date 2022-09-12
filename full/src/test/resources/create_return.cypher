CREATE (n:Node {id:1}) RETURN n;
CREATE (start:Start {id:1})-[rel:REL {id: 2}]->(end:End {id: 3}) RETURN start, rel, end;
UNWIND range(1,2) as idx WITH idx CREATE path=(:StartPath {id:idx})-[:REL_PATH {id: idx}]->(:EndPath {id: idx}) RETURN path;
UNWIND range(1,2) as idx WITH idx CREATE path=(:StartPath {id:idx})-[:REL_PATH {id: idx}]->(:EndPath {id: idx}) RETURN collect(path) as paths;