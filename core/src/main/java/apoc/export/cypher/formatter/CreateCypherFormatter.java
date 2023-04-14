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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class CreateCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {


    @Override
    public String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        result.append("CREATE (");
        String labels = CypherFormatterUtils.formatAllLabels(node, uniqueConstraints, indexNames);
        if (!labels.isEmpty()) {
            result.append(labels);
        }
        result.append(" {");
        result.append(CypherFormatterUtils.formatNodeProperties("", node, uniqueConstraints, indexNames, true));
        result.append("}");
        result.append(");");
        return result.toString();
    }

    @Override
    public String statementForRelationship(Relationship relationship,  Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, ExportConfig exportConfig) {
        StringBuilder result = new StringBuilder(100);
        result.append("MATCH ");
        result.append(CypherFormatterUtils.formatNodeLookup("n1", relationship.getStartNode(), uniqueConstraints, indexedProperties));
        result.append(", ");
        result.append(CypherFormatterUtils.formatNodeLookup("n2", relationship.getEndNode(), uniqueConstraints, indexedProperties));
        result.append(" CREATE (n1)-[r:" + CypherFormatterUtils.quote(relationship.getType().name()));
        if (relationship.getPropertyKeys().iterator().hasNext()) {
            result.append(" {");
            result.append(CypherFormatterUtils.formatRelationshipProperties("", relationship, true));
            result.append("}");
        }
        result.append("]->(n2);");
        return result.toString();
    }

    @Override
    public void statementForNodes(Iterable<Node> node, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db) {
        buildStatementForNodes("CREATE ", "SET ", node, uniqueConstraints, exportConfig, out, reporter, db);
    }

    @Override
    public void statementForRelationships(Iterable<Relationship> relationship, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db) {
        buildStatementForRelationships("CREATE ", "SET ", relationship, uniqueConstraints, exportConfig, out, reporter, db);
    }

}