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
import apoc.export.util.ExportFormatExtended;
import apoc.export.util.ReporterExtended;
import apoc.util.UtilExtended;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintType;

import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.export.cypher.formatter.CypherFormatterUtilsExtended.Q_UNIQUE_ID_LABEL;
import static apoc.export.cypher.formatter.CypherFormatterUtilsExtended.Q_UNIQUE_ID_REL;
import static apoc.export.cypher.formatter.CypherFormatterUtilsExtended.UNIQUE_ID_PROP;
import static apoc.export.cypher.formatter.CypherFormatterUtilsExtended.isUniqueRelationship;
import static apoc.export.cypher.formatter.CypherFormatterUtilsExtended.simpleKeyValue;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
abstract class AbstractCypherFormatterExtended implements CypherFormatterExtended {

    private static final String STATEMENT_CONSTRAINTS = "CREATE CONSTRAINT %s%s FOR (node:%s) REQUIRE (%s) %s;";
    private static final String STATEMENT_CONSTRAINTS_REL =
            "CREATE CONSTRAINT %s%s FOR ()-[rel:%s]-() REQUIRE (%s) %s;";
    private static final String STATEMENT_DROP_CONSTRAINTS = "DROP CONSTRAINT %s;";

    private static final String STATEMENT_NODE_FULLTEXT_IDX = "CREATE FULLTEXT INDEX %s FOR (n:%s) ON EACH [%s];";
    private static final String STATEMENT_REL_FULLTEXT_IDX =
            "CREATE FULLTEXT INDEX %s FOR ()-[rel:%s]-() ON EACH [%s];";
    public static final String PROPERTY_QUOTING_FORMAT = "%s.`%s`";
    private static final String ID_REL_KEY = "id";

    @Override
    public String statementForCleanUpNodes(int batchSize) {
        return "MATCH (n:" + Q_UNIQUE_ID_LABEL + ") " + " WITH n LIMIT "
                + batchSize + " REMOVE n:"
                + Q_UNIQUE_ID_LABEL + " REMOVE n." + UtilExtended.quote(UNIQUE_ID_PROP) + ";";
    }

    @Override
    public String statementForCleanUpRelationships(int batchSize) {
        return "MATCH ()-[r]->() WHERE r." + Q_UNIQUE_ID_REL + " IS NOT NULL"
                + " WITH r LIMIT " + batchSize
                + " REMOVE r." + Q_UNIQUE_ID_REL + ";";
    }

    @Override
    public String statementForNodeIndex(
            String indexType, String label, Iterable<String> keys, boolean ifNotExists, String idxName) {
        return String.format(
                "CREATE %s INDEX%s%s FOR (n:%s) ON (%s);",
                indexType, idxName, getIfNotExists(ifNotExists), UtilExtended.quote(label), getPropertiesQuoted(keys, "n."));
    }

    @Override
    public String statementForIndexRelationship(
            String indexType, String type, Iterable<String> keys, boolean ifNotExists, String idxName) {
        return String.format(
                "CREATE %s INDEX%s%s FOR ()-[rel:%s]-() ON (%s);",
                indexType, idxName, getIfNotExists(ifNotExists), UtilExtended.quote(type), getPropertiesQuoted(keys, "rel."));
    }

    @Override
    public String statementForNodeFullTextIndex(String name, Iterable<Label> labels, Iterable<String> keys) {
        String label = StreamSupport.stream(labels.spliterator(), false)
                .map(Label::name)
                .map(UtilExtended::quote)
                .collect(Collectors.joining("|"));
        String key = StreamSupport.stream(keys.spliterator(), false)
                .map(s -> String.format(PROPERTY_QUOTING_FORMAT, "n", s))
                .collect(Collectors.joining(","));
        return String.format(STATEMENT_NODE_FULLTEXT_IDX, name, label, key);
    }

    @Override
    public String statementForRelationshipFullTextIndex(
            String name, Iterable<RelationshipType> types, Iterable<String> keys) {
        String type = StreamSupport.stream(types.spliterator(), false)
                .map(RelationshipType::name)
                .map(UtilExtended::quote)
                .collect(Collectors.joining("|"));
        String key = StreamSupport.stream(keys.spliterator(), false)
                .map(s -> String.format(PROPERTY_QUOTING_FORMAT, "rel", s))
                .collect(Collectors.joining(","));
        return String.format(STATEMENT_REL_FULLTEXT_IDX, name, type, key);
    }

    @Override
    public String statementForCreateConstraint(
            String name, String label, Iterable<String> keys, ConstraintType type, boolean ifNotExists) {
        String keysString = "";
        String typeString = "";
        String statement = "";
        switch (type) {
            case UNIQUENESS -> {
                keysString = "node.";
                typeString = "IS UNIQUE";
                statement = STATEMENT_CONSTRAINTS;
            }
            case NODE_KEY -> {
                keysString = "node.";
                typeString = "IS NODE KEY";
                statement = STATEMENT_CONSTRAINTS;
            }
            case NODE_PROPERTY_EXISTENCE -> {
                keysString = "node.";
                typeString = "IS NOT NULL";
                statement = STATEMENT_CONSTRAINTS;
            }
            case RELATIONSHIP_UNIQUENESS -> {
                keysString = "rel.";
                typeString = "IS UNIQUE";
                statement = STATEMENT_CONSTRAINTS_REL;
            }
            case RELATIONSHIP_KEY -> {
                keysString = "rel.";
                typeString = "IS NODE KEY";
                statement = STATEMENT_CONSTRAINTS_REL;
            }
            case RELATIONSHIP_PROPERTY_EXISTENCE -> {
                keysString = "rel.";
                typeString = "IS NOT NULL";
                statement = STATEMENT_CONSTRAINTS_REL;
            }
        }

        return String.format(
                statement,
                UtilExtended.quote(name),
                getIfNotExists(ifNotExists),
                UtilExtended.quote(label),
                getPropertiesQuoted(keys, keysString),
                typeString);
    }

    @Override
    public String statementForDropConstraint(String name) {
        return String.format(STATEMENT_DROP_CONSTRAINTS, UtilExtended.quote(name));
    }

    private String getIfNotExists(boolean ifNotExists) {
        return ifNotExists ? " IF NOT EXISTS" : "";
    }

    private String getPropertiesQuoted(Iterable<String> keys, String prefix) {
        String keysString = StreamSupport.stream(keys.spliterator(), false)
                .map(key -> prefix + UtilExtended.quote(key))
                .collect(Collectors.joining(", "));
        return keysString;
    }

    protected String mergeStatementForNode(
            CypherFormatExtended cypherFormat,
            Node node,
            Map<String, Set<String>> uniqueConstraints,
            Set<String> indexedProperties,
            Set<String> indexNames) {
        StringBuilder result = new StringBuilder(1000);
        result.append("MERGE ");
        result.append(CypherFormatterUtilsExtended.formatNodeLookup("n", node, uniqueConstraints, indexNames));
        String notUniqueProperties =
                CypherFormatterUtilsExtended.formatNotUniqueProperties("n", node, uniqueConstraints, indexedProperties, false);
        String notUniqueLabels = CypherFormatterUtilsExtended.formatNotUniqueLabels("n", node, uniqueConstraints);
        if (!notUniqueProperties.isEmpty() || !notUniqueLabels.isEmpty()) {
            result.append(cypherFormat.equals(CypherFormatExtended.ADD_STRUCTURE) ? " ON CREATE SET " : " SET ");
            result.append(notUniqueProperties);
            result.append(!"".equals(notUniqueProperties) && !"".equals(notUniqueLabels) ? ", " : "");
            result.append(notUniqueLabels);
        }
        result.append(";");
        return result.toString();
    }

    public String mergeStatementForRelationship(
            CypherFormatExtended cypherFormat,
            Relationship relationship,
            Map<String, Set<String>> uniqueConstraints,
            Set<String> indexedProperties,
            ExportConfigExtended exportConfig) {
        StringBuilder result = new StringBuilder(1000);
        result.append("MATCH ");
        final Node startNode = relationship.getStartNode();
        result.append(CypherFormatterUtilsExtended.formatNodeLookup("n1", startNode, uniqueConstraints, indexedProperties));
        result.append(", ");
        final Node endNode = relationship.getEndNode();
        result.append(CypherFormatterUtilsExtended.formatNodeLookup("n2", endNode, uniqueConstraints, indexedProperties));
        final RelationshipType type = relationship.getType();
        final boolean withMultiRels =
                exportConfig.isMultipleRelationshipsWithType() && !isUniqueRelationship(relationship);
        String mergeUniqueKey = withMultiRels ? simpleKeyValue(Q_UNIQUE_ID_REL, relationship.getId()) : "";
        result.append(" MERGE (n1)-[r:" + UtilExtended.quote(type.name()) + mergeUniqueKey + "]->(n2)");
        if (relationship.getPropertyKeys().iterator().hasNext()) {
            result.append(cypherFormat.equals(CypherFormatExtended.UPDATE_STRUCTURE) ? " ON CREATE SET " : " SET ");
            result.append(CypherFormatterUtilsExtended.formatRelationshipProperties("r", relationship, false));
        }
        result.append(";");
        return result.toString();
    }

    public void buildStatementForNodes(
            String nodeClause,
            String setClause,
            Iterable<Node> nodes,
            Map<String, Set<String>> uniqueConstraints,
            ExportConfigExtended exportConfig,
            PrintWriter out,
            ReporterExtended reporter,
            GraphDatabaseService db) {
        // Batch stream results, process BATCH_COUNT nodes at a time
        boolean shouldContinue = true;
        AtomicInteger totalNodeCount = new AtomicInteger(0);
        while (shouldContinue) {
            AtomicInteger nodesInBatch = new AtomicInteger(0);
            Function<Node, Map.Entry<Set<String>, Set<String>>> keyMapper = (node) -> {
                try (Transaction tx = db.beginTx()) {
                    totalNodeCount.incrementAndGet();
                    nodesInBatch.incrementAndGet();
                    node = tx.getNodeByElementId(node.getElementId());
                    Set<String> idProperties = CypherFormatterUtilsExtended.getNodeIdProperties(node, uniqueConstraints)
                            .keySet();
                    Set<String> labels = getLabels(node);
                    tx.commit();
                    return new AbstractMap.SimpleImmutableEntry<>(labels, idProperties);
                }
            };
            Map<Map.Entry<Set<String>, Set<String>>, List<Node>> groupedData = StreamSupport.stream(
                            com.google.common.collect.Iterables.limit(
                                            com.google.common.collect.Iterables.skip(nodes, totalNodeCount.get()),
                                            exportConfig.getBatchSize())
                                    .spliterator(),
                            true)
                    .collect(Collectors.groupingByConcurrent(keyMapper));

            // Each loop will collect at most the batch size in nodes to process
            // This is done using a limit on the stream. If the limit returns less than
            // the batch size, this means we have no more nodes to process after this round and
            // can stop.
            if (nodesInBatch.get() < exportConfig.getBatchSize()) shouldContinue = false;

            AtomicInteger propertiesCount = new AtomicInteger(0);

            AtomicInteger batchCount = new AtomicInteger(0);
            AtomicInteger nodeCount = new AtomicInteger(0);
            groupedData.forEach((key, nodeList) -> {
                AtomicInteger unwindCount = new AtomicInteger(0);
                final int nodeListSize = nodeList.size();
                final Node last = nodeList.get(nodeListSize - 1);
                nodeCount.addAndGet(nodeListSize);
                for (Node node : nodeList) {
                    writeBatchBegin(exportConfig, out, batchCount);
                    writeUnwindStart(exportConfig, out, unwindCount);
                    batchCount.incrementAndGet();
                    unwindCount.incrementAndGet();
                    Map<String, Object> props = node.getAllProperties();
                    // start element
                    out.append("{");

                    // id
                    Map<String, Object> idMap = CypherFormatterUtilsExtended.getNodeIdProperties(node, uniqueConstraints);
                    writeNodeIds(out, idMap);

                    // properties
                    out.append(", ");
                    out.append("properties:");

                    propertiesCount.addAndGet(props.size());
                    props.keySet().removeAll(idMap.keySet());
                    writeProperties(out, props);

                    // end element
                    out.append("}");
                    if (last.equals(node)
                            || isBatchMatch(exportConfig, batchCount)
                            || isUnwindBatchMatch(exportConfig, unwindCount)) {
                        closeUnwindNodes(nodeClause, setClause, uniqueConstraints, exportConfig, out, key, last);
                        writeBatchEnd(exportConfig, out, batchCount);
                        unwindCount.set(0);
                    } else {
                        out.append(", ");
                    }
                }
            });
            addCommitToEnd(exportConfig, out, batchCount);

            reporter.update(nodeCount.get(), 0, propertiesCount.longValue());
        }
    }

    private void closeUnwindNodes(
            String nodeClause,
            String setClause,
            Map<String, Set<String>> uniqueConstraints,
            ExportConfigExtended exportConfig,
            PrintWriter out,
            Map.Entry<Set<String>, Set<String>> key,
            Node last) {
        writeUnwindEnd(exportConfig, out);
        out.append(StringUtils.LF);
        out.append(nodeClause);

        String label = getUniqueConstrainedLabel(last, uniqueConstraints);
        out.append("(n:");
        out.append(UtilExtended.quote(label));
        out.append("{");
        writeSetProperties(out, key.getValue());
        out.append("}) ");
        out.append(setClause);
        out.append("n += row.properties");
        String addLabels = key.getKey().stream()
                .filter(l -> !l.equals(label))
                .map(UtilExtended::quote)
                .collect(Collectors.joining(":"));
        if (!addLabels.isEmpty()) {
            out.append(" SET n:");
            out.append(addLabels);
        }
        out.append(";");
        out.append(StringUtils.LF);
    }

    private void writeSetProperties(PrintWriter out, Set<String> value) {
        writeSetProperties(out, value, null);
    }

    private void writeSetProperties(PrintWriter out, Set<String> value, String prefix) {
        if (prefix == null) prefix = "";
        int size = value.size();
        for (String s : value) {
            --size;
            out.append(UtilExtended.quote(s) + ": row." + prefix + formatNodeId(s));
            if (size > 0) {
                out.append(", ");
            }
        }
    }

    private boolean isBatchMatch(ExportConfigExtended exportConfig, AtomicInteger batchCount) {
        return batchCount.get() % exportConfig.getBatchSize() == 0;
    }

    public void buildStatementForRelationships(
            String relationshipClause,
            String setClause,
            Iterable<Relationship> relationship,
            Map<String, Set<String>> uniqueConstraints,
            ExportConfigExtended exportConfig,
            PrintWriter out,
            ReporterExtended reporter,
            GraphDatabaseService db) {
        boolean shouldContinue = true;
        AtomicInteger totalRelCount = new AtomicInteger(0);
        while (shouldContinue) {
            AtomicInteger relsInBatch = new AtomicInteger(0);

            Function<Relationship, Map<String, Object>> keyMapper = (rel) -> {
                totalRelCount.incrementAndGet();
                relsInBatch.incrementAndGet();
                try (Transaction tx = db.beginTx()) {
                    rel = tx.getRelationshipByElementId(rel.getElementId());
                    Node start = rel.getStartNode();
                    Set<String> startLabels = getLabels(start);

                    // define the end labels
                    Node end = rel.getEndNode();
                    Set<String> endLabels = getLabels(end);

                    // define the type
                    String type = rel.getType().name();

                    // create the path
                    Map<String, Object> key = UtilExtended.map(
                            "type",
                            type,
                            "start",
                            new AbstractMap.SimpleImmutableEntry<>(
                                    startLabels,
                                    CypherFormatterUtilsExtended.getNodeIdProperties(start, uniqueConstraints)
                                            .keySet()),
                            "end",
                            new AbstractMap.SimpleImmutableEntry<>(
                                    endLabels,
                                    CypherFormatterUtilsExtended.getNodeIdProperties(end, uniqueConstraints)
                                            .keySet()));

                    tx.commit();
                    return key;
                }
            };
            Map<Map<String, Object>, List<Relationship>> groupedData = StreamSupport.stream(
                            com.google.common.collect.Iterables.limit(
                                            com.google.common.collect.Iterables.skip(relationship, totalRelCount.get()),
                                            exportConfig.getBatchSize())
                                    .spliterator(),
                            true)
                    .collect(Collectors.groupingByConcurrent(keyMapper));

            // Each loop will collect at most the batch size in rels to process
            // This is done using a limit on the stream. If the limit returns less than
            // the batch size, this means we have no more rels to process after this round and
            // can stop.
            if (relsInBatch.get() < exportConfig.getBatchSize()) shouldContinue = false;

            AtomicInteger propertiesCount = new AtomicInteger(0);
            AtomicInteger batchCount = new AtomicInteger(0);

            String start = "start";
            String end = "end";
            AtomicInteger relCount = new AtomicInteger(0);
            groupedData.forEach((path, relationshipList) -> {
                AtomicInteger unwindCount = new AtomicInteger(0);
                final int relSize = relationshipList.size();
                relCount.addAndGet(relSize);
                final Relationship last = relationshipList.get(relSize - 1);
                for (Relationship rel : relationshipList) {
                    writeBatchBegin(exportConfig, out, batchCount);
                    writeUnwindStart(exportConfig, out, unwindCount);
                    batchCount.incrementAndGet();
                    unwindCount.incrementAndGet();
                    Map<String, Object> props = rel.getAllProperties();
                    // start element
                    out.append("{");

                    // start node
                    Node startNode = rel.getStartNode();
                    writeRelationshipNodeIds(uniqueConstraints, out, start, startNode);

                    Node endNode = rel.getEndNode();
                    final boolean withMultipleRels = exportConfig.isMultipleRelationshipsWithType();
                    out.append(", ");
                    if (withMultipleRels) {
                        String uniqueId = String.format("%s: %s, ", ID_REL_KEY, rel.getId());
                        out.append(uniqueId);
                    }

                    // end node
                    writeRelationshipNodeIds(uniqueConstraints, out, end, endNode);

                    // properties
                    out.append(", ");
                    out.append("properties:");
                    writeProperties(out, props);
                    propertiesCount.addAndGet(props.size());

                    // end element
                    out.append("}");

                    if (last.equals(rel)
                            || isBatchMatch(exportConfig, batchCount)
                            || isUnwindBatchMatch(exportConfig, unwindCount)) {
                        closeUnwindRelationships(
                                relationshipClause,
                                setClause,
                                uniqueConstraints,
                                exportConfig,
                                out,
                                start,
                                end,
                                path,
                                last,
                                withMultipleRels);
                        writeBatchEnd(exportConfig, out, batchCount);
                        unwindCount.set(0);
                    } else {
                        out.append(", ");
                    }
                }
            });
            addCommitToEnd(exportConfig, out, batchCount);

            reporter.update(0, relCount.get(), propertiesCount.longValue());
        }
    }

    private void closeUnwindRelationships(
            String relationshipClause,
            String setClause,
            Map<String, Set<String>> uniqueConstraints,
            ExportConfigExtended exportConfig,
            PrintWriter out,
            String start,
            String end,
            Map<String, Object> path,
            Relationship last,
            boolean withMultipleRels) {
        writeUnwindEnd(exportConfig, out);
        // match start node
        writeRelationshipMatchAsciiNode(last.getStartNode(), out, start, uniqueConstraints);

        // match end node
        writeRelationshipMatchAsciiNode(last.getEndNode(), out, end, uniqueConstraints);

        out.append(StringUtils.LF);

        // create the relationship (depends on the strategy)
        out.append(relationshipClause);
        String mergeUniqueKey = withMultipleRels ? simpleKeyValue(Q_UNIQUE_ID_REL, "row." + ID_REL_KEY) : "";
        out.append("(start)-[r:" + UtilExtended.quote(path.get("type").toString()) + mergeUniqueKey + "]->(end) ");
        out.append(setClause);
        out.append("r += row.properties;");
        out.append(StringUtils.LF);
    }

    private boolean isUnwindBatchMatch(ExportConfigExtended exportConfig, AtomicInteger batchCount) {
        return batchCount.get() % exportConfig.getUnwindBatchSize() == 0;
    }

    private void writeBatchEnd(ExportConfigExtended exportConfig, PrintWriter out, AtomicInteger batchCount) {
        if (isBatchMatch(exportConfig, batchCount)) {
            out.append(exportConfig.getFormat().commit());
        }
    }

    public void writeProperties(PrintWriter out, Map<String, Object> props) {
        out.append("{");
        if (!props.isEmpty()) {
            int size = props.size();
            for (Map.Entry<String, Object> es : props.entrySet()) {
                --size;
                out.append(UtilExtended.quote(es.getKey()));
                out.append(":");
                out.append(CypherFormatterUtilsExtended.toString(es.getValue()));
                if (size > 0) {
                    out.append(", ");
                }
            }
        }
        out.append("}");
    }

    private String formatNodeId(String key) {
        if (CypherFormatterUtilsExtended.UNIQUE_ID_PROP.equals(key)) {
            key = "_id";
        }
        return UtilExtended.quote(key);
    }

    private void addCommitToEnd(ExportConfigExtended exportConfig, PrintWriter out, AtomicInteger batchCount) {
        if (batchCount.get() % exportConfig.getBatchSize() != 0) {
            out.append(exportConfig.getFormat().commit());
        }
    }

    private void writeBatchBegin(ExportConfigExtended exportConfig, PrintWriter out, AtomicInteger batchCount) {
        if (isBatchMatch(exportConfig, batchCount)) {
            out.append(exportConfig.getFormat().begin());
        }
    }

    private void writeUnwindStart(ExportConfigExtended exportConfig, PrintWriter out, AtomicInteger batchCount) {
        if (isUnwindBatchMatch(exportConfig, batchCount)) {
            String start = (exportConfig.getFormat() == ExportFormatExtended.CYPHER_SHELL
                            && exportConfig.getOptimizationType() == ExportConfigExtended.OptimizationType.UNWIND_BATCH_PARAMS)
                    ? ":param rows => ["
                    : "UNWIND [";
            out.append(start);
        }
    }

    private void writeUnwindEnd(ExportConfigExtended exportConfig, PrintWriter out) {
        out.append("]");
        if (exportConfig.getFormat() == ExportFormatExtended.CYPHER_SHELL
                && exportConfig.getOptimizationType() == ExportConfigExtended.OptimizationType.UNWIND_BATCH_PARAMS) {
            out.append(StringUtils.LF);
            out.append("UNWIND $rows");
        }
        out.append(" AS row");
    }

    private String getUniqueConstrainedLabel(Node node, Map<String, Set<String>> uniqueConstraints) {
        return uniqueConstraints.entrySet().stream()
                .filter(e -> node.hasLabel(Label.label(e.getKey()))
                        && e.getValue().stream().anyMatch(k -> node.hasProperty(k)))
                .map(e -> e.getKey())
                .findFirst()
                .orElse(CypherFormatterUtilsExtended.UNIQUE_ID_LABEL);
    }

    private Set<String> getUniqueConstrainedProperties(
            Map<String, Set<String>> uniqueConstraints, String uniqueConstrainedLabel) {
        Set<String> props = uniqueConstraints.get(uniqueConstrainedLabel);
        if (props == null || props.isEmpty()) {
            props = Collections.singleton(UNIQUE_ID_PROP);
        }
        return props;
    }

    private Set<String> getLabels(Node node) {
        Set<String> labels = StreamSupport.stream(node.getLabels().spliterator(), false)
                .map(Label::name)
                .collect(Collectors.toSet());
        if (labels.isEmpty()) {
            labels.add(CypherFormatterUtilsExtended.UNIQUE_ID_LABEL);
        }
        return labels;
    }

    private void writeRelationshipMatchAsciiNode(
            Node node, PrintWriter out, String key, Map<String, Set<String>> uniqueConstraints) {
        String uniqueConstrainedLabel = getUniqueConstrainedLabel(node, uniqueConstraints);
        Set<String> uniqueConstrainedProps = getUniqueConstrainedProperties(uniqueConstraints, uniqueConstrainedLabel);

        out.append(StringUtils.LF);
        out.append("MATCH ");
        out.append("(");
        out.append(key);
        out.append(":");
        out.append(UtilExtended.quote(uniqueConstrainedLabel));
        out.append("{");
        writeSetProperties(out, uniqueConstrainedProps, key + ".");
        out.append("})");
    }

    private void writeRelationshipNodeIds(
            Map<String, Set<String>> uniqueConstraints, PrintWriter out, String key, Node node) {
        String uniqueConstrainedLabel = getUniqueConstrainedLabel(node, uniqueConstraints);
        Set<String> props = getUniqueConstrainedProperties(uniqueConstraints, uniqueConstrainedLabel);
        Map<String, Object> properties;
        if (!props.contains(UNIQUE_ID_PROP)) {
            String[] propsArray = props.toArray(new String[props.size()]);
            properties = node.getProperties(propsArray);
        } else {
            // UNIQUE_ID_PROP is always the only member of the Set
            properties = UtilExtended.map(UNIQUE_ID_PROP, node.getId());
        }

        out.append(key + ": ");
        out.append("{");
        writeNodeIds(out, properties);
        out.append("}");
    }

    private void writeNodeIds(PrintWriter out, Map<String, Object> properties) {
        int size = properties.size();
        for (Map.Entry<String, Object> es : properties.entrySet()) {
            --size;
            out.append(formatNodeId(es.getKey()));
            out.append(":");
            out.append(CypherFormatterUtilsExtended.toString(es.getValue()));
            if (size > 0) {
                out.append(", ");
            }
        }
    }
}
