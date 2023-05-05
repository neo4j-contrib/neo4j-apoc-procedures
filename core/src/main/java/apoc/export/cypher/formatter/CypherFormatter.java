/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.export.cypher.formatter;

import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import org.neo4j.graphdb.*;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import org.neo4j.graphdb.schema.ConstraintType;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public interface CypherFormatter {

	String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames);

	String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, ExportConfig exportConfig);

	String statementForNodeIndex(String label, Iterable<String> keys, boolean ifNotExist, String idxName);

	String statementForIndexRelationship(String type, Iterable<String> keys, boolean ifNotExist, String idxName);

	String statementForNodeFullTextIndex(String name, Iterable<Label> labels, Iterable<String> keys);

	String statementForRelationshipFullTextIndex(String name, Iterable<RelationshipType> types, Iterable<String> keys);

	String statementForConstraint(String label, Iterable<String> keys, ConstraintType type, boolean ifNotExist, String name);

	String statementForCleanUp(int batchSize);

	void statementForNodes(Iterable<Node> node, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db);

	void statementForRelationships(Iterable<Relationship> relationship, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db);

}
