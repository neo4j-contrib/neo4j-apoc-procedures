CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE;
CREATE (m:Person {name: 'Michael Jordan', age: 54});
CREATE (q:Person {name: 'Tom Burton', age: 23})
CREATE (p:Person {name: 'John William', age: 22})
CREATE (q)-[:KNOWS{since:2016, time:time('125035.556+0100')}]->(p);