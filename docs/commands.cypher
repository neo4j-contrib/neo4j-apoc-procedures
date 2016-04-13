-- tag::mysql[]
select count(*) from products;
+----------+
| count(*) |
+----------+
|       77 |
+----------+
1 row in set (0,00 sec)

describe products;
+-----------------+---------------+------+-----+---------+----------------+
| Field           | Type          | Null | Key | Default | Extra          |
+-----------------+---------------+------+-----+---------+----------------+
| ProductID       | int(11)       | NO   | PRI | NULL    | auto_increment |
| ProductName     | varchar(40)   | NO   | MUL | NULL    |                |
| SupplierID      | int(11)       | YES  | MUL | NULL    |                |
| CategoryID      | int(11)       | YES  | MUL | NULL    |                |
| QuantityPerUnit | varchar(20)   | YES  |     | NULL    |                |
| UnitPrice       | decimal(10,4) | YES  |     | 0.0000  |                |
| UnitsInStock    | smallint(2)   | YES  |     | 0       |                |
| UnitsOnOrder    | smallint(2)   | YES  |     | 0       |                |
| ReorderLevel    | smallint(2)   | YES  |     | 0       |                |
| Discontinued    | bit(1)        | NO   |     | b'0'    |                |
+-----------------+---------------+------+-----+---------+----------------+
10 rows in set (0,00 sec)
-- end::mysql[]

tag::jdbc[]
cypher CALL apoc.load.driver("com.mysql.jdbc.Driver");
cypher CALL apoc.load.jdbc("jdbc:mysql://localhost:3306/northwind?user=root","products") YIELD row 
RETURN count(*);
cypher CALL apoc.load.jdbc("jdbc:mysql://localhost:3306/northwind?user=root","products") YIELD row 
RETURN row limit 1;

// tag::jdbc[]
cypher CALL apoc.load.driver("com.mysql.jdbc.Driver");

CALL apoc.load.jdbc("jdbc:mysql://localhost:3306/northwind?user=root","products") YIELD row 
RETURN count(*);
+----------+
| count(*) |
+----------+
| 77       |
+----------+
1 row
23 ms

CALL apoc.load.jdbc("jdbc:mysql://localhost:3306/northwind?user=root","products") YIELD row 
RETURN row limit 1;
+--------------------------------------------------------------------------------+
| row                                                                            |
+--------------------------------------------------------------------------------+
| {UnitPrice -> 18.0000, UnitsOnOrder -> 0, CategoryID -> 1, UnitsInStock -> 39} |
+--------------------------------------------------------------------------------+
1 row
10 ms
// end::jdbc[]