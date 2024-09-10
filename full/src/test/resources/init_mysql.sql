-- MySQL dump 10.13  Distrib 5.1.51, for pc-linux-gnu (i686)
--
-- Host: 127.0.0.1    Database: world
-- ------------------------------------------------------
SET AUTOCOMMIT=0;

-- Table structure for table `country`
--

DROP TABLE IF EXISTS `country`;
CREATE TABLE `country` (
  `Code` CHAR(3) NOT NULL DEFAULT '',
  `Name` CHAR(52) NOT NULL DEFAULT '',
  `Continent` enum('Asia','Europe','North America','Africa','Oceania','Antarctica','South America') NOT NULL DEFAULT 'Asia',
  `Region` CHAR(26) NOT NULL DEFAULT '',
  `SurfaceArea` FLOAT(10,2) NOT NULL DEFAULT '0.00',
  `IndepYear` SMALLINT(6) DEFAULT NULL,
  `Population` INT(11) NOT NULL DEFAULT '0',
  `LifeExpectancy` FLOAT(3,1) DEFAULT NULL,
  `GNP` FLOAT(10,2) DEFAULT NULL,
  `GNPOld` FLOAT(10,2) DEFAULT NULL,
  `LocalName` CHAR(45) NOT NULL DEFAULT '',
  `GovernmentForm` CHAR(45) NOT NULL DEFAULT '',
  `HeadOfState` CHAR(60) DEFAULT NULL,
  `Capital` INT(11) DEFAULT NULL,
  `Code2` CHAR(2) NOT NULL DEFAULT '',
  `myTime` TIME NOT NULL DEFAULT '0',
  `myDateTime` DATETIME NOT NULL,
  `myTimeStamp` TIMESTAMP NOT NULL,
  `myDate` DATE NOT NULL,
  `myYear` YEAR NOT NULL,
  PRIMARY KEY (`Code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `country`
--
-- ORDER BY:  `Code`

INSERT INTO `country` VALUES ('NLD','Netherlands','Europe','Western Europe',41526.00,1581,15864000,78.3,371362.00,360478.00,'Nederland','Constitutional Monarchy','Beatrix',5,'NL',
TIME('01:00:00'),
DATE('2003-01-01 01:00:00'),
TIMESTAMP('2003-01-01 01:00:00'),
DATE('2003-01-01 01:00:00'),
YEAR('2003-01-01 01:00:00')
);
COMMIT;

--
-- Table structure for table `city`
--

DROP TABLE IF EXISTS `city`;
CREATE TABLE `city` (
  `ID` INT(11) NOT NULL AUTO_INCREMENT,
  `Name` CHAR(35) NOT NULL DEFAULT '',
  `CountryCode` CHAR(3) NOT NULL DEFAULT '',
  `District` CHAR(20) NOT NULL DEFAULT '',
  `Population` INT(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`ID`),
  KEY `CountryCode` (`CountryCode`),
  CONSTRAINT `city_ibfk_1` FOREIGN KEY (`CountryCode`) REFERENCES `country` (`Code`)
) ENGINE=InnoDB AUTO_INCREMENT=4080 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `city`
--
-- ORDER BY:  `ID`

INSERT INTO `city` VALUES (8,'Utrecht','NLD','Utrecht',234323);
COMMIT;
--
-- Table structure for table `countrylanguage`
--

DROP TABLE IF EXISTS `countrylanguage`;
CREATE TABLE `countrylanguage` (
  `CountryCode` CHAR(3) NOT NULL DEFAULT '',
  `Language` CHAR(30) NOT NULL DEFAULT '',
  `IsOfficial` enum('T','F') NOT NULL DEFAULT 'F',
  `Percentage` FLOAT(4,1) NOT NULL DEFAULT '0.0',
  PRIMARY KEY (`CountryCode`,`Language`),
  KEY `CountryCode` (`CountryCode`),
  CONSTRAINT `countryLanguage_ibfk_1` FOREIGN KEY (`CountryCode`) REFERENCES `country` (`Code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `countrylanguage`
--
-- ORDER BY:  `CountryCode`,`Language`

INSERT INTO `countrylanguage` VALUES ('NLD','Arabic','F',0.9);
INSERT INTO `countrylanguage` VALUES ('NLD','Dutch','T',95.6);
INSERT INTO `countrylanguage` VALUES ('NLD','Fries','F',3.7);
INSERT INTO `countrylanguage` VALUES ('NLD','Turkish','F',0.8);
COMMIT;

SET AUTOCOMMIT=1;
