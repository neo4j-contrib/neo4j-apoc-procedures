DROP TABLE IF EXISTS country;
CREATE TABLE country (
    code character(3) primary key,
    name text NOT NULL,
    continent text NOT NULL,
    region text NOT NULL,
    surface_area real NOT NULL,
    indep_year smallint,
    population integer NOT NULL,
    life_expectancy real,
    gnp numeric(10,2),
    gnp_old numeric(10,2),
    local_name text NOT NULL,
    government_form text NOT NULL,
    head_of_state text,
    capital integer,
    code2 character(2) NOT NULL,
    CONSTRAINT country_continent_check CHECK ((((((((continent = 'Asia'::text) OR (continent = 'Europe'::text)) OR (continent = 'North America'::text)) OR (continent = 'Africa'::text)) OR (continent = 'Oceania'::text)) OR (continent = 'Antarctica'::text)) OR (continent = 'South America'::text)))
);

--
-- Dumping data for table country
--
-- ORDER BY:  Code

INSERT INTO country VALUES ('NLD','Netherlands','Europe','Western Europe',41526.00,1581,15864000,78.3,371362.00,360478.00,'Nederland','Constitutional Monarchy','Beatrix',5,'NL');
COMMIT;

--
-- Table structure for table city
--

DROP TABLE IF EXISTS city;
CREATE TABLE city (
    id integer primary key,
    name text NOT NULL,
    country_code character(3) NOT NULL,
    district text NOT NULL,
    population integer NOT NULL
);

--
-- Dumping data for table city
--
-- ORDER BY:  ID

INSERT INTO city VALUES (8,'Utrecht','NLD','Utrecht',234323);
COMMIT;
--
-- Table structure for table countrylanguage
--

DROP TABLE IF EXISTS country_language;
CREATE TABLE country_language (
    country_code character(3) NOT NULL,
    "language" text NOT NULL,
    is_official boolean NOT NULL,
    percentage real NOT NULL
);

--
-- Dumping data for table countrylanguage
--
-- ORDER BY:  CountryCode,Language

INSERT INTO country_language VALUES ('NLD','Arabic','F',0.9);
INSERT INTO country_language VALUES ('NLD','Dutch','T',95.6);
INSERT INTO country_language VALUES ('NLD','Fries','F',3.7);
INSERT INTO country_language VALUES ('NLD','Turkish','F',0.8);
COMMIT;


CREATE TABLE PERSON (
    "NAME" varchar(50),
    "SURNAME" varchar(50),
    "HIRE_DATE" DATE,
    "EFFECTIVE_FROM_DATE" TIMESTAMP with time zone,
    "TEST_TIME" TIME,
    "NULL_DATE" DATE);

INSERT INTO PERSON ("NAME", "SURNAME", "HIRE_DATE", "EFFECTIVE_FROM_DATE", "TEST_TIME", "NULL_DATE")
VALUES ('John', null, '2017-05-25', '2016-06-22 19:10:25+02', '15:37', null);

CREATE TABLE ARRAY_TABLE (
    "NAME" text,
    "INT_VALUES" int[],
    "DOUBLE_VALUES" DOUBLE PRECISION[]
);
INSERT INTO ARRAY_TABLE ("NAME", "INT_VALUES", "DOUBLE_VALUES")
VALUES ('John', '{ 1, 2, 3}', '{ 1.0, 2.0, 3.0 }');

CREATE TABLE nodes (id serial PRIMARY KEY, my_id integer);
