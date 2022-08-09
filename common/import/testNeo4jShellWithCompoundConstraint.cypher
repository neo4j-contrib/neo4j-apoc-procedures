BEGIN
CREATE CONSTRAINT PersonRequiresNamesConstraint FOR (node:Person) REQUIRE (node.name, node.surname) IS NODE KEY;
COMMIT
SCHEMA AWAIT
BEGIN
UNWIND [{surname:"Snow", name:"John", properties:{}}, {surname:"Jackson", name:"Matt", properties:{}}, {surname:"White", name:"Jenny", properties:{}}, {surname:"Brown", name:"Susan", properties:{}}, {surname:"Taylor", name:"Tom", properties:{}}] AS row
CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;
COMMIT
BEGIN
UNWIND [{start: {name:"John", surname:"Snow"}, end: {name:"Matt", surname:"Jackson"}, properties:{}}] AS row
MATCH (start:Person{surname: row.start.surname, name: row.start.name})
MATCH (end:Person{surname: row.end.surname, name: row.end.name})
CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
COMMIT
