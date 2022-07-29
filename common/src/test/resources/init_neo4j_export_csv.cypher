CREATE CONSTRAINT PersonRequiresNamesConstraint FOR (t:Person) REQUIRE (t.name, t.surname) IS NODE KEY;

CREATE (a:Person {name: 'John', surname: 'Snow'})
CREATE (b:Person {name: 'Matt', surname: 'Jackson'})
CREATE (c:Person {name: 'Jenny', surname: 'White'})
CREATE (d:Person {name: 'Susan', surname: 'Brown'})
CREATE (e:Person {name: 'Tom', surname: 'Taylor'})
CREATE (a)-[:KNOWS]->(b);