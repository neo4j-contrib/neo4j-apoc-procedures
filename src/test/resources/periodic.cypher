USING PERIODIC COMMIT 1
LOAD CSV WITH HEADERS FROM "file:///test.dsv" AS row FIELDTERMINATOR ":"
CREATE (n:Person {name:row.name, age:row.age});
