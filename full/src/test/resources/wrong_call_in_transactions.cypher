MATCH (n:Fail)
CALL {
    WITH n
    CREATE (:Fail {foo: 1})
} IN TRANSACTIONS OF 1 ROW;

