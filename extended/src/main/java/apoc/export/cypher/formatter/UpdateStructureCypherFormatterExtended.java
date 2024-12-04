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

import apoc.export.util.ExportConfigExtended;
import apoc.export.util.ReporterExtended;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.ConstraintType;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public class UpdateStructureCypherFormatterExtended extends AbstractCypherFormatterExtended implements CypherFormatterExtended {

    @Override
    public String statementForNode(
            Node node,
            Map<String, Set<String>> uniqueConstraints,
            Set<String> indexedProperties,
            Set<String> indexNames) {
        return "";
    }

    @Override
    public String statementForRelationship(
            Relationship relationship,
            Map<String, Set<String>> uniqueConstraints,
            Set<String> indexedProperties,
            ExportConfigExtended exportConfig) {
        return super.mergeStatementForRelationship(
                CypherFormatExtended.UPDATE_STRUCTURE, relationship, uniqueConstraints, indexedProperties, exportConfig);
    }

    @Override
    public String statementForCleanUpNodes(int batchSize) {
        return "";
    }

    @Override
    public String statementForNodeIndex(
            String indexType, String label, Iterable<String> key, boolean ifNotExist, String idxName) {
        return "";
    }

    @Override
    public String statementForIndexRelationship(
            String indexType, String type, Iterable<String> key, boolean ifNotExist, String idxName) {
        return "";
    }

    @Override
    public String statementForCreateConstraint(
            String name, String label, Iterable<String> key, ConstraintType type, boolean ifNotExist) {
        return "";
    }

    @Override
    public String statementForDropConstraint(String name) {
        return "";
    }

    @Override
    public void statementForNodes(
            Iterable<Node> node,
            Map<String, Set<String>> uniqueConstraints,
            ExportConfigExtended exportConfig,
            PrintWriter out,
            ReporterExtended reporter,
            GraphDatabaseService db) {}

    @Override
    public void statementForRelationships(
            Iterable<Relationship> relationship,
            Map<String, Set<String>> uniqueConstraints,
            ExportConfigExtended exportConfig,
            PrintWriter out,
            ReporterExtended reporter,
            GraphDatabaseService db) {
        buildStatementForRelationships(
                "MERGE ", "SET ", relationship, uniqueConstraints, exportConfig, out, reporter, db);
    }
}
