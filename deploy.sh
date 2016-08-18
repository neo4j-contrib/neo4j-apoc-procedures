#!bin/bash
mvn clean install -DskipTests
echo "Copying jar file"
cp target/apoc-1.2.0-SNAPSHOT.jar ../neo4j-community-3.0.3/plugins/
echo "Restarting neo4j server .. "
~/Documents/neo4j-community-3.0.3/bin/neo4j restart
echo "Done"
