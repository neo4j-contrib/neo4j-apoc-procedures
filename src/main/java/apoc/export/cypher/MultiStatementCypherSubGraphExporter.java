package apoc.export.cypher;

import apoc.export.cypher.formatter.CypherFormatter;
import apoc.export.cypher.formatter.CypherFormatterUtils;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportFormat;
import apoc.export.util.Reporter;
import apoc.util.Util;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;

import java.io.IOException;
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

    private final SubGraph            graph;
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
    public void export(ExportConfig config, Reporter reporter, ExportFileManager cypherFileManager) throws IOException {

        int batchSize = config.getBatchSize();
        ExportConfig.OptimizationType useOptimizations = config.getOptimizationType();

        switch (useOptimizations) {
            case NONE:
                exportNodes(cypherFileManager.getPrintWriter("nodes"), reporter, batchSize);
                exportSchema(cypherFileManager.getPrintWriter("schema"));
                exportRelationships(cypherFileManager.getPrintWriter("relationships"), reporter, batchSize);
                break;
            default:
                artificialUniques += countArtificialUniques(graph.getNodes());
                exportSchema(cypherFileManager.getPrintWriter("schema"));
                PrintWriter nodeWrite = cypherFileManager.getPrintWriter("nodes");
                exportNodesUnwindBatch(nodeWrite, reporter);
                PrintWriter relWrite = cypherFileManager.getPrintWriter("relationships");
                exportRelationshipsUnwindBatch(relWrite, reporter);
        }
        exportCleanUp(cypherFileManager.getPrintWriter("cleanup"), batchSize);
        reporter.done();
    }

    public void exportOnlySchema(ExportFileManager cypherFileManager) throws IOException {
        exportSchema(cypherFileManager.getPrintWriter("schema"));
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
        List<String> indexes = exportIndexes();
        if (indexes.isEmpty() && artificialUniques == 0) return;
        begin(out);
        for (String index : indexes) {
            out.println(index);
        }
        if (artificialUniques > 0) {
            String cypher = this.cypherFormat.statementForConstraint(UNIQUE_ID_LABEL, Collections.singleton(UNIQUE_ID_PROP));
            if (cypher != null && !"".equals(cypher)) {
                out.println(cypher);
            }
        }
        commit(out);
        List<String> indexesAwait = indexesAwait();
        for (String indexAwait : indexesAwait) {
            out.print(indexAwait);
        }
        schemaAwait(out);
        out.flush();
    }

    private List<String> exportIndexes() {
        List<String> result = new ArrayList<>();
        for (IndexDefinition index : graph.getIndexes()) {
            String label = index.getLabel().name();
            Iterable<String> props = index.getPropertyKeys();
            if (index.isConstraintIndex()) {
                String cypher = this.cypherFormat.statementForConstraint(label, props);
                if (cypher != null && !"".equals(cypher)) {
                    result.add(cypher);
                }
            } else {
                String cypher = this.cypherFormat.statementForIndex(label, props);
                if (cypher != null && !"".equals(cypher)) {
                    result.add(0, cypher);
                }
            }
        }
        return result;
    }

    private List<String> indexesAwait() {
        List<String> result = new ArrayList<>();
        for (IndexDefinition index : graph.getIndexes()) {
            String label = index.getLabel().name();
            String indexAwait = this.exportFormat.indexAwait(label, index.getPropertyKeys());
            if (indexAwait != null && !"".equals(indexAwait))
                result.add(indexAwait);
        }
        return result;
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
            String label = indexDefinition.getLabel().name();
            Set<String> props = StreamSupport
                    .stream(indexDefinition.getPropertyKeys().spliterator(), false)
                    .collect(Collectors.toSet());
            indexNames.add(label);
            indexedProperties.addAll(props);
            if (indexDefinition.isConstraintIndex()) { // we use the constraint that have few properties
                uniqueConstraints.compute(label, (k, v) ->  v == null || v.size() > props.size() ? props : v);
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