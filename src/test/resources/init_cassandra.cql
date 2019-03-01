CREATE KEYSPACE playlist WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
USE playlist;

create table person (id int primary key, name text, exist boolean, effective_from_date timestamp, double_col double, null_date date);

insert into person ("id", "name", "exist", "effective_from_date", "double_col", "null_date") values (1,'John',true,'2016-06-22T19:10:25+0200',1.2,null);