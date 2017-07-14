#!/bin/bash
# Script to execute the Apoc expor cypher tests

neo4j_home=$1
user=$2
password=$3
errorMessage="Please set the parameter <YOUR NEO4J HOME> <-u USERNAME> <-p PASSWORD> [-a ADDRESS]"
dropDb="match(n) detach delete n"

if [[ "$1" == "" || "$2" == "" || "$3" == "" ]]; then
	echo $errorMessage
	exit
fi
if [[ "$4" != "" ]]; then
    url="$4"
else
    url='bolt://localhost:7687'
fi

cd $neo4j_home/bin

#Export all no config params
export exportAllNoConfig="call apoc.export.graphml.all('$neo4j_home/import/exportAll.graphml',null)"
echo $exportAllNoConfig
time ./cypher-shell -a $url -u $user -p $password "$exportAllNoConfig"

#Export all useTypes
export exportAllUseTypes="call apoc.export.graphml.all('$neo4j_home/import/exportAll.graphml',{useTypes:true})"
echo $exportAllUseTypes
time ./cypher-shell -a $url -u $user -p $password "$exportAllUseTypes"

#Export all storeNodeIds
export exportAllStoreNodeIds="call apoc.export.graphml.all('$neo4j_home/import/exportAll.graphml',{storeNodeIds:true})"
echo $exportAllStoreNodeIds
time ./cypher-shell -a $url -u $user -p $password "$exportAllStoreNodeIds"

#Export all defaultRelationshipType
export exportAllDefaultRelationshipType="call apoc.export.graphml.all('$neo4j_home/import/exportAll.graphml',{defaultRelationshipType:'RELATED'})"
echo $exportAllDefaultRelationshipType
time ./cypher-shell -a $url -u $user -p $password "$exportAllDefaultRelationshipType"

#Export all full config
export exportAllFullConfig="call apoc.export.graphml.all('$neo4j_home/import/exportAll.graphml',{batchSize: 10000, readLabels: true, storeNodeIds: false, defaultRelationshipType:'RELATED'})"
echo $exportAllFullConfig
time ./cypher-shell -a $url -u $user -p $password "$exportAllFullConfig"

#Export GraphML from query no config
export exportQuery="call apoc.export.graphml.query('MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m','$neo4j_home/import/exportQuery.graphml',null)"
echo $exportQuery
time ./cypher-shell -a $url -u $user -p $password "$exportQuery"

#Export GraphML from query useTypes
export exportQueryUseTypes="call apoc.export.graphml.query('MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m','$neo4j_home/import/exportQuery.graphml',{useTypes:true})"
echo $exportQueryUseTypes
time ./cypher-shell -a $url -u $user -p $password "$exportQueryUseTypes"

#Export GraphML from Graph object no config
export exportGraph="call apoc.graph.fromDB('test',{}) yield graph CALL apoc.export.graphml.graph(graph, '$neo4j_home/import/exportGraph.graphml',null) YIELD nodes, relationships return nodes, relationships" > "$neo4j_home/import/exportGraphML"
echo $exportGraph
time ./cypher-shell -a $url -u $user -p $password "$exportGraph"

#Export GraphML from Graph object useTypes
export exportGraphUseTypes="call apoc.graph.fromDB('test',{}) yield graph CALL apoc.export.graphml.graph(graph, '$neo4j_home/import/exportGraph.graphml',{useTypes:true}) YIELD nodes, relationships return nodes, relationships" > "$neo4j_home/import/exportGraphML"
echo $exportGraphUseTypes
time ./cypher-shell -a $url -u $user -p $password "$exportGraphUseTypes"

#Export GraphML from given nodes and rels no config
export exportData="Match (n:Person)-[r:LIKES_COMMENT]->(c:Comment) with collect(n) as colN, collect(c) as colC, collect(r) as colR CALL apoc.export.cypher.data(colN+colC,colR, '$neo4j_home/import/exportData.graphml',null) yield nodes, relationships return nodes, relationships" > "$neo4j_home/import/exportDataResult"
echo $exportData
time ./cypher-shell -a $url -u $user -p $password "$exportData"

#Export GraphML from given nodes and rels useTypes
export exportDataUseTypes="match (n:Person)-[r:LIKES_COMMENT]->(c:Comment) CALL apoc.export.graphml.data([n],[r], '$neo4j_home/import/exportData.graphml',{useTypes:true}) yield nodes, relationships return nodes, relationships" > "$neo4j_home/import/exportDataResult"
echo $exportDataUseTypes
time ./cypher-shell -a $url -u $user -p $password "$exportDataUseTypes"

#Drop DB
echo "drop db"
time ./cypher-shell -a $url -u $user -p $password "$dropDb"

#Import GraphML
export importGraphML="call apoc.import.graphml('$neo4j_home/import/exportAllNoConfig.graphml',{batchSize: 10000})"
echo $importGraphML
time ./cypher-shell -a $url -u $user -p $password "$importGraphML"
