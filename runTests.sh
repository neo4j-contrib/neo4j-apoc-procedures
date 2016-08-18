#!bin/sh
mvn clean install -DskipTests
mvn surefire:test -Dtest=apoc.algo.PageRankTest
