LOAD CSV WITH HEADERS FROM "file:///test.dsv" AS row FIELDTERMINATOR ":"
CALL {
    WITH row
    CREATE (n:Person {name:row.name, age:row.age})
} IN TRANSACTIONS OF 1 ROW;

