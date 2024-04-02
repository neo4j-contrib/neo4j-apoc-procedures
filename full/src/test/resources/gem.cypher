MATCH (n:Result)-[:REL]->(:Other)
SET n.updated = true
RETURN n;
