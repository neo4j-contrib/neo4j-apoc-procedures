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

#Export all batch size default
export exportAllDefaultBatch="call apoc.export.cypher.all('$neo4j_home/import/exportAll.cypher', {useOptimizations: {type: "none"}])"
echo $exportAllDefaultBatch
time ./cypher-shell -a $url -u $user -p $password "$exportAllDefaultBatch"

#Export all batch size 10000
export exportAllBatchSize10000="call apoc.export.cypher.all('$neo4j_home/import/exportAll.cypher',  {batchSize: 10000, useOptimizations: {type: "none"}})"
echo $exportAllBatchSize10000
time ./cypher-shell -a $url -u $user -p $password "$exportAllBatchSize10000"

#Export all batch size 1000
export exportAllBatchSize1000="call apoc.export.cypher.all('$neo4j_home/import/exportAll.cypher',  {batchSize: 1000, useOptimizations: {type: "none"}})"
echo $exportAllBatchSize1000
time ./cypher-shell -a $url -u $user -p $password "$exportAllBatchSize1000"

#Export all batch size 100
export exportAllBatchSize100="call apoc.export.cypher.all('$neo4j_home/import/exportAll.cypher',  {batchSize: 100, useOptimizations: {type: "none"}})"
echo $exportAllBatchSize100
time ./cypher-shell -a $url -u $user -p $password "$exportAllBatchSize100"

#Different output formats
#Neo4j Shell
export exportAllNeo4jShell="call apoc.export.cypher.all('$neo4j_home/import/exportAllNeo4jShell.cypher',  {format:'neo4j-shell', useOptimizations: {type: "none"}})"
echo $exportAllNeo4jShell
time ./cypher-shell -a $url -u $user -p $password  "$exportAllNeo4jShell"

#Cypher Shell
export exportAllCypherShell="call apoc.export.cypher.all('$neo4j_home/import/exportAllCypherShell.cypher',  {format:'cypher-shell', useOptimizations: {type: "none"}})"
echo $exportAllCypherShell
time ./cypher-shell -a $url -u $user -p $password  "$exportAllCypherShell"

#Plain
export exportAllPlain="call apoc.export.cypher.all('$neo4j_home/import/exportAllPlain.cypher',  {format:'plain',useOptimizations: {type: "none"}})"
echo $exportAllPlain
time ./cypher-shell -a $url -u $user -p $password  "$exportAllPlain"

#NEW METHOD

#Neo4j Shell
export exportAllNeo4jShellOptimized="call apoc.export.cypher.all('$neo4j_home/import/exportAllNeo4jShellOptimized.cypher',  {format:'neo4j-shell'})"
echo $exportAllNeo4jShellOptimized
time ./cypher-shell -a $url -u $user -p $password  "$exportAllNeo4jShellOptimized"

#Cypher Shell
export exportAllCypherShellOptimized="call apoc.export.cypher.all('$neo4j_home/import/exportAllCypherShellOptimized.cypher',  {format:'cypher-shell'})"
echo $exportAllCypherShellOptimized
time ./cypher-shell -a $url -u $user -p $password  "$exportAllCypherShellOptimized"

#Plain
export exportAllPlainOptimized="call apoc.export.cypher.all('$neo4j_home/import/exportAllPlainOptimized.cypher',  {format:'plain'})"
echo $exportAllPlainOptimized
time ./cypher-shell -a $url -u $user -p $password  "$exportAllPlainOptimized"

#Many files
export exportAllSeparateFiles="call apoc.export.cypher.all('$neo4j_home/import/exportAllDifferentFile.cypher',  {useOptimizations: {type: "none"}, format:'plain', separateFiles:true})"
echo $exportAllSeparateFiles
time ./cypher-shell -a $url -u $user -p $password  "$exportAllSeparateFiles"

#Export from query
export exportQuery="call apoc.export.cypher.query('MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m', '$neo4j_home/import/exportQuery.cypher', {useOptimizations: {type: "none"}})"
echo $exportQuery
time ./cypher-shell -a $url -u $user -p $password  "$exportQuery"

#Export from given nodes and rels
export exportData="Match (n:Person)-[r:LIKES_COMMENT]->(c:Comment) with collect(n) as colN, collect(c) as colC, collect(r) as colR CALL apoc.export.cypher.data(colN+colC,colR, '$neo4j_home/import/exportData.cypher',{format:'plain', batchSize:1000}) YIELD nodes, relationships RETURN nodes, relationships"
time ./cypher-shell -a $url -u $user -p $password "$exportData"

#Export from graph object
export exportGraph="CALL apoc.graph.fromDB('test',{}) yield graph CALL apoc.export.cypher.graph(graph, '$neo4j_home/import/exportGraph.cypher',{useOptimizations: {type: "none"}}) YIELD nodes, relationships RETURN nodes, relationships"
echo $exportGraph
time ./cypher-shell -a $url -u $user -p $password "$exportGraph"

#Graph Cypher shell format
export exportGraphCypherShell="CALL apoc.graph.fromDB('test',{}) yield graph CALL apoc.export.cypher.graph(graph, '$neo4j_home/import/exportGraph.cypher',{format:'cypher-shell', useOptimizations: {type: "none"}}) YIELD nodes, relationships RETURN nodes, relationships"
echo $exportGraphCypherShell
time ./cypher-shell -a $url -u $user -p $password "$exportGraphCypherShell"

#Graph batch size 1000
export exportGraphBatchSize1000="CALL apoc.graph.fromDB('test',{}) yield graph CALL apoc.export.cypher.graph(graph, '$neo4j_home/import/exportGraph.cypher',{batchSize: 1000, useOptimizations: {type: "none"}}) YIELD nodes, relationships RETURN nodes, relationships"
echo $exportGraphBatchSize1000
time ./cypher-shell -a $url -u $user -p $password "$exportGraphBatchSize1000"

#Drop DB
echo "drop db"
time ./cypher-shell -a $url -u $user -p $password "$dropDb"

#run Schema file
export importRunSchemaFile="call apoc.cypher.runSchemaFile('$neo4j_home/import/exportAllDifferentFile.schema.cypher')"
echo $importRunSchemaFile
time ./cypher-shell -a $url -u $user -p $password "$importRunSchemaFile"

#run file node
export importRunFileNode="call apoc.cypher.runFile('$neo4j_home/import/exportAllDifferentFile.nodes.cypher')"
echo $importRunFileNode
time ./cypher-shell -a $url -u $user -p $password "$importRunFileNode"

#run file rel
export importRunFileRel="call apoc.cypher.runFile('$neo4j_home/import/exportAllDifferentFile.relationships.cypher')"
echo $importRunFileRel
time ./cypher-shell -a $url -u $user -p $password "$importRunFileRel"

#Drop DB
echo "drop db"
time ./cypher-shell -a $url -u $user -p $password "$dropDb"

#import cypher-shell
export importCypherShell="$neo4j_home/import/exportAllCypherShell.cypher"
export outputFile="$neo4j_home/import/cypherShellOutput"
echo $importCypherShell "<" $outputFile
time ./cypher-shell -a $url -u $user -p $password < "$importCypherShell">"$outputFile"


#import cypher-shell Optimized
export importCypherShell="$neo4j_home/import/exportAllCypherShellOptimized.cypher"
export outputFile="$neo4j_home/import/cypherShellOutput"
echo $importCypherShell "<" $outputFile
time ./cypher-shell -a $url -u $user -p $password < "$importCypherShell">"$outputFile"

#Drop DB
echo "drop db"
time ./cypher-shell -a $url -u $user -p $password "$dropDb"

#import neo4j-shell
export importNeo4jShell="$neo4j_home/import/exportAllNeo4jShell.cypher"
export outputFile="$neo4j_home/import/neo4jShellOutput"
echo $importNeo4jShell "<" $outputFile
time ./neo4j-shell -a $url -u $user -p $password '-file' "$importNeo4jShell">"$outputFile"

#import neo4j-shell Optimized
export importNeo4jShell="$neo4j_home/import/exportAllNeo4jShellOptimized.cypher"
export outputFile="$neo4j_home/import/neo4jShellOutput"
echo $importNeo4jShell "<" $outputFile
time ./neo4j-shell -a $url -u $user -p $password '-file' "$importNeo4jShell">"$outputFile"
