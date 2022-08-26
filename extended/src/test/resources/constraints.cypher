CREATE CONSTRAINT uniqueConstraint FOR (n:Person) REQUIRE n.name IS UNIQUE;
CREATE CONSTRAINT another_cons FOR (n:AnotherLabel) REQUIRE n.name IS UNIQUE;