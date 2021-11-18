package apoc.export.cypher;

import apoc.export.cypher.formatter.CypherFormatter;
import apoc.export.cypher.formatter.CypherFormatterUtils;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportFormat;
import apoc.export.util.Reporter;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterables;

import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static apoc.export.cypher.formatter.CypherFormatterUtils.UNIQUE_ID_LABEL;
import static apoc.export.cypher.formatter.CypherFormatterUtils.UNIQUE_ID_PROP;

/*
 * Idea is to lookup nodes for relationships via a unique index
 * either one inherent to the original node, or a artificial one that indexes the original node-id
 * and which is removed after the import.
 * <p>
 * Outputs indexes and constraints at the beginning as their own transactions
 */
public class MultiStatementCypherSubGraphExporter {

    /*private enum IndexType {
        NODE_LABEL_PROPERTY("node_label_property"),
        NODE_UNIQUE_PROPERTY("node_unique_property"),
        REL_TYPE_PROPERTY("relationship_type_property"),
        NODE_FULLTEXT("node_fulltext"),
        RELATIONSHIP_FULLTEXT("relationship_fulltext");

        private final String typeName;

        IndexType(String typeName) {
            this.typeName = typeName;
        }

        static IndexType from(String type, String entityType, String uniqueness) {
            if (uniqueness.equals("UNIQUE") && entityType.equals("NODE")) {
                return NODE_UNIQUE_PROPERTY
            }

            return Stream.of(IndexType.values()).filter(type -> type.typeName().equals(stringType)).findFirst().orElseThrow();
        }

        public String typeName() {
            return typeName;
        }
    }*/

    private final SubGraph graph;
    private final Map<String, Set<String>> uniqueConstraints = new HashMap<>();
    private Set<String> indexNames        = new LinkedHashSet<>();
    private Set<String> indexedProperties = new LinkedHashSet<>();
    private Long artificialUniques = 0L;

    private ExportFormat exportFormat;
    private CypherFormatter cypherFormat;
    private ExportConfig exportConfig;
    private GraphDatabaseService db;

    public MultiStatementCypherSubGraphExporter(SubGraph graph, ExportConfig config, GraphDatabaseService db) {
        this.graph = graph;
        this.exportFormat = config.getFormat();
        this.exportConfig = config;
        this.cypherFormat = config.getCypherFormat().getFormatter();
        this.db = db;
        gatherUniqueConstraints();
    }

    /**
     * Given a full path file name like <code>/tmp/myexport.cypher</code>,
     * when <code>ExportConfig#separateFiles() == true</code>,
     * this method will create the following files:
     * <ul>
     * <li>/tmp/myexport.nodes.cypher</li>
     * <li>/tmp/myexport.schema.cypher</li>
     * <li>/tmp/myexport.relationships.cypher</li>
     * <li>/tmp/myexport.cleanup.cypher</li>
     * </ul>
     * Otherwise all kernelTransaction will be saved in the original file.
     * @param config
     * @param reporter
     * @param cypherFileManager
     */
    public void export(ExportConfig config, Reporter reporter, ExportFileManager cypherFileManager) {

        int batchSize = config.getBatchSize();
        ExportConfig.OptimizationType useOptimizations = config.getOptimizationType();

        PrintWriter schemaWriter = cypherFileManager.getPrintWriter("schema");
        PrintWriter nodesWriter = cypherFileManager.getPrintWriter("nodes");
        PrintWriter relationshipsWriter = cypherFileManager.getPrintWriter("relationships");
        PrintWriter cleanupWriter = cypherFileManager.getPrintWriter("cleanup");

        switch (useOptimizations) {
            case NONE:
                exportNodes(nodesWriter, reporter, batchSize);
                exportSchema(schemaWriter);
                exportRelationships(relationshipsWriter, reporter, batchSize);
                break;
            default:
                artificialUniques += countArtificialUniques(graph.getNodes());
                exportSchema(schemaWriter);
                exportNodesUnwindBatch(nodesWriter, reporter);
                exportRelationshipsUnwindBatch(relationshipsWriter, reporter);
                break;
        }
        if (cypherFileManager.separatedFiles()) {
            nodesWriter.close();
            schemaWriter.close();
            relationshipsWriter.close();
        }
        exportCleanUp(cleanupWriter, batchSize);
        cleanupWriter.close();
        reporter.done();
    }

    public void exportOnlySchema(ExportFileManager cypherFileManager) {
        PrintWriter schemaWriter = cypherFileManager.getPrintWriter("schema");
        exportSchema(schemaWriter);
        schemaWriter.close();
    }

    // ---- Nodes ----

    private void exportNodes(PrintWriter out, Reporter reporter, int batchSize) {
        if (graph.getNodes().iterator().hasNext()) {
            begin(out);
            appendNodes(out, batchSize, reporter);
            commit(out);
            out.flush();
        }
    }

    private void exportNodesUnwindBatch(PrintWriter out, Reporter reporter) {
        if (graph.getNodes().iterator().hasNext()) {
            this.cypherFormat.statementForNodes(graph.getNodes(), uniqueConstraints, exportConfig, out, reporter, db);
            out.flush();
        }
    }

    private long appendNodes(PrintWriter out, int batchSize, Reporter reporter) {
        long count = 0;
        for (Node node : graph.getNodes()) {
            if (count > 0 && count % batchSize == 0) restart(out);
            count++;
            appendNode(out, node, reporter);
        }
        return count;
    }

    private void appendNode(PrintWriter out, Node node, Reporter reporter) {
        artificialUniques += countArtificialUniques(node);
        String cypher = this.cypherFormat.statementForNode(node, uniqueConstraints, indexedProperties, indexNames);
        if (Util.isNotNullOrEmpty(cypher)) {
            out.println(cypher);
            reporter.update(1, 0, Iterables.count(node.getPropertyKeys()));
        }
    }

    // ---- Relationships ----

    private void exportRelationships(PrintWriter out, Reporter reporter, int batchSize) {
        if (graph.getRelationships().iterator().hasNext()) {
            begin(out);
            appendRelationships(out, batchSize, reporter);
            commit(out);
            out.flush();
        }
    }

    private void exportRelationshipsUnwindBatch(PrintWriter out, Reporter reporter) {
        if (graph.getRelationships().iterator().hasNext()) {
            this.cypherFormat.statementForRelationships(graph.getRelationships(), uniqueConstraints, exportConfig, out, reporter, db);
            out.flush();
        }
    }

    private long appendRelationships(PrintWriter out, int batchSize, Reporter reporter) {
        long count = 0;
        for (Relationship rel : graph.getRelationships()) {
            if (count > 0 && count % batchSize == 0) restart(out);
            count++;
            appendRelationship(out, rel, reporter);
        }
        return count;
    }

    private void appendRelationship(PrintWriter out, Relationship rel, Reporter reporter) {
        String cypher = this.cypherFormat.statementForRelationship(rel, uniqueConstraints, indexedProperties);
        if (cypher != null && !"".equals(cypher)) {
            out.println(cypher);
            reporter.update(0, 1, Iterables.count(rel.getPropertyKeys()));
        }
    }

    // ---- Schema ----

    private void exportSchema(PrintWriter out) {
        List<String> indexesAndConstraints = new ArrayList<>();
        indexesAndConstraints.addAll(exportIndexes());
        indexesAndConstraints.addAll(exportConstraints());
        if (indexesAndConstraints.isEmpty() && artificialUniques == 0) return;
        begin(out);
        for (String index : indexesAndConstraints) {
            out.println(index);
        }
        if (artificialUniques > 0) {
            String cypher = this.cypherFormat.statementForConstraint(UNIQUE_ID_LABEL, Collections.singleton(UNIQUE_ID_PROP));
            if (cypher != null && !"".equals(cypher)) {
                out.println(cypher);
            }
        }
        commit(out);
        if (graph.getIndexes().iterator().hasNext()) {
            out.print(this.exportFormat.indexAwait(this.exportConfig.getAwaitForIndexes()));
        }
        schemaAwait(out);
        out.flush();
    }

    private List<String> exportIndexes() {
        return db.executeTransactionally("CALL db.indexes()", Collections.emptyMap(), result -> result.stream()
                .map(map -> {
                    if ("LOOKUP".equals(map.get("type"))) {
                        return "";
                    }
                    List<String> props = (List<String>) map.get("properties");
                    List<String> tokenNames = (List<String>) map.get("labelsOrTypes");
                    String name = (String) map.get("name");
                    boolean inGraph = tokensInGraph(tokenNames);
                    if (!inGraph) {
                        return null;
                    }

                    if ("UNIQUE".equals(map.get("uniqueness"))) {
                        return null;  // delegate to the constraint creation
                    }

                    boolean isNode = "NODE".equals(map.get("entityType"));
                    if ("FULLTEXT".equals(map.get("type"))) {
                        if (isNode) {
                            List<Label> labels = toLabels(tokenNames);
                            return this.cypherFormat.statementForNodeFullTextIndex(name, labels, props);
                        } else {
                            List<RelationshipType> types = toRelationshipTypes(tokenNames);
                            return this.cypherFormat.statementForRelationshipFullTextIndex(name, types, props);
                        }
                    }
                    // "normal" schema index
                    String tokenName = tokenNames.get(0);
                    if (isNode) {
                        return this.cypherFormat.statementForNodeIndex(tokenName, props);
                    } else {
                        return this.cypherFormat.statementForIndexRelationship(tokenName, props);
                    }

                })
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList()));
    }

    private boolean tokensInGraph(List<String> tokens) {
        return StreamSupport.stream(graph.getIndexes().spliterator(), false)
                .anyMatch(indexDefinition -> {
                    if (indexDefinition.isRelationshipIndex()) {
                        List<String> typeNames = StreamSupport.stream(indexDefinition.getRelationshipTypes().spliterator(), false)
                                .map(RelationshipType::name)
                                .collect(Collectors.toList());
                        return typeNames.containsAll(tokens);
                    } else {
                        List<String> labelNames = StreamSupport.stream(indexDefinition.getLabels().spliterator(), false)
                                .map(Label::name)
                                .collect(Collectors.toList());
                        return labelNames.containsAll(tokens);
                    }
                });
    }

    private List<Label> toLabels(List<String> tokenNames) {
        return tokenNames.stream()
                .map(Label::label)
                .collect(Collectors.toList());
    }

    private List<RelationshipType> toRelationshipTypes(List<String> tokenNames) {
        return tokenNames.stream()
                .map(RelationshipType::withName)
                .collect(Collectors.toList());
    }

    private List<String> exportConstraints() {
        return StreamSupport.stream(graph.getIndexes().spliterator(), false)
                .filter(index -> index.isConstraintIndex())
                .map(index -> {
                    String label = Iterables.single(index.getLabels()).name();
                    Iterable<String> props = index.getPropertyKeys();
                    return this.cypherFormat.statementForConstraint(label, props);
                })
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
    }

    // ---- CleanUp ----

    private void exportCleanUp(PrintWriter out, int batchSize) {
        if (artificialUniques > 0) {
            while (artificialUniques > 0) {
                String cypher = this.cypherFormat.statementForCleanUp(batchSize);
                begin(out);
                if (cypher != null && !"".equals(cypher)) {
                    out.println(cypher);
                }
                commit(out);
                artificialUniques -= batchSize;
            }
            begin(out);
            String cypher = this.cypherFormat.statementForConstraint(UNIQUE_ID_LABEL, Collections.singleton(UNIQUE_ID_PROP)).replaceAll("^CREATE", "DROP");
            if (cypher != null && !"".equals(cypher)) {
                out.println(cypher);
            }
            commit(out);
        }
        out.flush();
    }

    // ---- Common ----

    public void begin(PrintWriter out) {
        out.print(exportFormat.begin());
    }

    private void schemaAwait(PrintWriter out){
        out.print(exportFormat.schemaAwait());
    }

    private void restart(PrintWriter out) {
        commit(out);
        begin(out);
    }

    public void commit(PrintWriter out){
        out.print(exportFormat.commit());
    }

    private void gatherUniqueConstraints() {
        for (IndexDefinition indexDefinition : graph.getIndexes()) {
            if (!indexDefinition.isNodeIndex()) {
                continue;
            }
            if (indexDefinition.getIndexType() == IndexType.LOOKUP) {
                continue;
            }
            Set<String> label = StreamSupport.stream(indexDefinition.getLabels().spliterator(), false)
                    .map(Label::name)
                    .collect(Collectors.toSet());
            Set<String> props = StreamSupport
                    .stream(indexDefinition.getPropertyKeys().spliterator(), false)
                    .collect(Collectors.toSet());
            indexNames.add(indexDefinition.getName());
            indexedProperties.addAll(props);
            if (indexDefinition.isConstraintIndex()) { // we use the constraint that have few properties
                uniqueConstraints.compute(String.join(":", label), (k, v) ->  v == null || v.size() > props.size() ? props : v);
            }
        }
    }

    private long countArtificialUniques(Node node) {
        long artificialUniques = 0;
        artificialUniques = getArtificialUniques(node, artificialUniques);
        return artificialUniques;
    }

    private long countArtificialUniques(Iterable<Node> n) {
        long artificialUniques = 0;
        for (Node node : n) {
            artificialUniques = getArtificialUniques(node, artificialUniques);
        }
        return artificialUniques;
    }

    private long getArtificialUniques(Node node, long artificialUniques) {
        Iterator<Label> labels = node.getLabels().iterator();
        boolean uniqueFound = false;
        while (labels.hasNext()) {
            Label next = labels.next();
            String labelName = next.name();
            uniqueFound = CypherFormatterUtils.isUniqueLabelFound(node, uniqueConstraints, labelName);

        }
        if (!uniqueFound) {
            artificialUniques++;
        }
        return artificialUniques;
    }
}
