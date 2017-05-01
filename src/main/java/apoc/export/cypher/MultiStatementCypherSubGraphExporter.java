package apoc.export.cypher;

import apoc.export.util.*;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.*;

/*
 * Idea is to lookup nodes for relationships via a unqiue index
 * either one inherent to the original node, or a artificial one that indexes the original node-id
 * and which is removed after the import.
 * <p>
 * Outputs indexes and constraints at the beginning as their own transactions
 */
public class MultiStatementCypherSubGraphExporter {
    private final static String UNIQUE_ID_LABEL = "UNIQUE IMPORT LABEL";
    private final static String Q_UNIQUE_ID_LABEL = quote(UNIQUE_ID_LABEL);
    private final static String UNIQUE_ID_PROP = "UNIQUE IMPORT ID";
    private final SubGraph            graph;
    private final Map<String, String> uniqueConstraints;
    Set<String> indexNames        = new LinkedHashSet<>();
    Set<String> indexedProperties = new LinkedHashSet<>();
    private long artificialUniques = 0;
    private ExportFormat exportFormat = ExportFormat.NEO4J_SHELL;

    public MultiStatementCypherSubGraphExporter(SubGraph graph, ExportFormat exportFormat) {
        this.graph = graph;
        uniqueConstraints = gatherUniqueConstraints(indexNames, indexedProperties);
        this.exportFormat = exportFormat != null ? exportFormat : ExportFormat.NEO4J_SHELL;
    }

    public static String quote(String id) {
        return "`" + id + "`";
    }

    public static String label(String id) {
        return ":`" + id + "`";
    }

    /**
     * @param fileName
     * @param suffix
     * @return
     */
    private String normalizeFileName(final String fileName, String suffix) {
        // TODO check if this should be follow the same rules of FileUtils.readerFor
        return fileName.replace(".cypher", suffix != null ? "." + suffix + ".cypher" : ".cypher");
    }

    /**
     * @param fileName
     * @param suffix
     * @return
     * @throws IOException
     */
    private PrintWriter createWriter(String fileName, String suffix) throws IOException {
        return FileUtils.getPrintWriter(normalizeFileName(fileName, suffix), null);
    }

    /**
     * Given a full path file name like /tmp/myexport.cypher this method will create:
     * <ul>
     * <li>/tmp/myexport.schema.cypher</li>
     * <li>/tmp/myexport.nodes.cypher</li>
     * <li>/tmp/myexport.relationships.cypher</li>
     * <li>/tmp/myexport.cleanup.cypher</li>
     * </ul>
     *
     * @param fileName full path where all the files will be created
     * @param config
     * @param reporter
     */
    public void export(String fileName, ExportConfig config, Reporter reporter) throws IOException {
        int batchSize = config.getBatchSize();

        PrintWriter nodeWriter = null, schemaWriter = null, relationshipsWriter = null, cleanUpWriter = null;

        if (config.separateFiles()) {
            nodeWriter = createWriter(fileName, "nodes");
            schemaWriter = createWriter(fileName, "schema");
            relationshipsWriter = createWriter(fileName, "relationships");
            cleanUpWriter = createWriter(fileName, "cleanup");
        }
        else {
            // The same writer --> there is only one file
            nodeWriter = createWriter(fileName, null);
            schemaWriter = nodeWriter;
            relationshipsWriter = nodeWriter;
            cleanUpWriter = nodeWriter;
        }

        boolean hasNodes = hasData(graph.getNodes());
        if (hasNodes) {
            begin(nodeWriter);
            appendNodes(nodeWriter, batchSize, reporter);
            commit(nodeWriter);

            nodeWriter.flush();
        }
        writeMetaInformation(schemaWriter);
        schemaWriter.flush();

        if (hasData(graph.getRelationships())) {

            begin(relationshipsWriter);
            appendRelationships(relationshipsWriter, batchSize, reporter);
            commit(relationshipsWriter);

            relationshipsWriter.flush();
        }
        if (artificialUniques > 0) {
            removeArtificialMetadata(cleanUpWriter, batchSize);
        }
        cleanUpWriter.flush();
    }

    private boolean hasData(Iterable<?> it) {
        return it.iterator().hasNext();
    }

    private Map<String, String> gatherUniqueConstraints(Set<String> indexes, Set<String> indexedProperties) {
        Map<String, String> result = new HashMap<>();
        for (IndexDefinition indexDefinition : graph.getIndexes()) {
            String label = indexDefinition.getLabel().name();
            String prop = Iterables.first(indexDefinition.getPropertyKeys());
            indexes.add(label);
            indexedProperties.add(prop);
            if (indexDefinition.isConstraintIndex()) {
                if (!result.containsKey(label)) result.put(label, prop);
            }
        }
        return result;
    }

    private void writeMetaInformation(PrintWriter out) {
        Collection<String> indexes = exportIndexes();
        if (indexes.isEmpty() && artificialUniques == 0) return;

		begin(out);
        for (String index : indexes) {
            out.println(index);
        }
        if (artificialUniques > 0) {
            out.println(uniqueConstraint(UNIQUE_ID_LABEL, UNIQUE_ID_PROP));
        }
        commit(out);
        schemaAwait(out);
    }
	
	private void begin(PrintWriter out) {
		out.print(exportFormat.begin());
	}

	private void schemaAwait(PrintWriter out){
		out.print(exportFormat.schemaAwait());
	}
	
    private void restart(PrintWriter out) {
		commit(out);
        begin(out);
    }

    private void removeArtificialMetadata(PrintWriter out, int batchSize) {
        while (artificialUniques > 0) {
			begin(out);
            out.println("MATCH (n:" + Q_UNIQUE_ID_LABEL + ") " +
                    " WITH n LIMIT " + batchSize +
                    " REMOVE n:" + Q_UNIQUE_ID_LABEL + " REMOVE n." + quote(UNIQUE_ID_PROP) + ";");
			commit(out);
            artificialUniques -= batchSize;
        }
		begin(out);
        out.println(uniqueConstraint(UNIQUE_ID_LABEL, UNIQUE_ID_PROP).replaceAll("^CREATE", "DROP"));
		commit(out);
    }

	private void commit(PrintWriter out){
		out.print(exportFormat.commit());
	}

    private Collection<String> exportIndexes() {
        List<String> result = new ArrayList<>();
        for (IndexDefinition index : graph.getIndexes()) {
            String label = index.getLabel().name();
            String prop = Iterables.single(index.getPropertyKeys());
            if (index.isConstraintIndex()) {
                result.add(uniqueConstraint(label, prop));
            } else {
                result.add(0, index(label, prop));
            }
        }
        return result;
    }

    private String index(String label, String key) {
        return "CREATE INDEX ON :" + quote(label) + "(" + quote(key) + ");";
    }

    private String uniqueConstraint(String label, String key) {
        return "CREATE CONSTRAINT ON (node:" + quote(label) + ") ASSERT node." + quote(key) + " IS UNIQUE;";
    }

    private boolean hasProperties(PropertyContainer node) {
        return node.getPropertyKeys().iterator().hasNext();
    }

    private String labelString(Node node) {
        Iterator<Label> labels = node.getLabels().iterator();
        StringBuilder result = new StringBuilder(100);
        boolean uniqueFound = false;
        while (labels.hasNext()) {
            Label next = labels.next();
            String labelName = next.name();
            if (uniqueConstraints.containsKey(labelName) && node.hasProperty(uniqueConstraints.get(labelName))) uniqueFound = true;
            if (indexNames.contains(labelName))
                result.insert(0, label(labelName));
            else
                result.append(label(labelName));
        }
        if (!uniqueFound) {
            result.append(label(UNIQUE_ID_LABEL));
            artificialUniques++;
        }
        return result.toString();
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

    // match (n1),(n2) where id(n1) = 234 and id(n2) = 345 create (n1)-[:TYPE {props}]->(n2);
    // match (n1:` Import Node ` {` import id `:234}),(n2:` Import Node ` {` import id `:345}) create (n1)-[:TYPE {props}]->(n2);
    private void appendRelationship(PrintWriter out, Relationship rel, Reporter reporter) {
        out.print("MATCH ");
        out.print(nodeLookup("n1", rel.getStartNode()));
        out.print(", ");
        out.print(nodeLookup("n2", rel.getEndNode()));
        out.print(" CREATE (n1)-[:");
        out.print(quote(rel.getType().name()));
        long props = formatProperties(out, rel, null);
        out.println("]->(n2);");
        reporter.update(0, 1, props);
    }

    private String nodeLookup(String id, Node node) {
        for (Label l : node.getLabels()) {
            String label = l.name();
            String prop = uniqueConstraints.get(label);
            if (prop == null) continue;
            Object value = node.getProperty(prop, null);
            if (value == null) continue;
            return nodeLookup(id, label, prop, value);
        }
        return nodeLookup(id, UNIQUE_ID_LABEL, UNIQUE_ID_PROP, node.getId());
    }

    private String nodeLookup(String id, String label, String prop, Object value) {
        return "(" + id + ":" + quote(label) + "{" + quote(prop) + ":" + toString(value) + "})";
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
        out.print("CREATE (");
        String labels = labelString(node);
        if (!labels.isEmpty()) {
            out.print(labels);
        }
        Long id = labels.endsWith(label(UNIQUE_ID_LABEL)) ? node.getId() : null;
        long props = formatProperties(out, node, id);
        out.println(");");
        reporter.update(1, 0, props);
    }

    private long formatProperties(PrintWriter out, PropertyContainer pc, Long id) {
        if (!hasProperties(pc) && id == null) return 0;
        out.print(" ");
        final String propertyString = formatProperties(pc, id);
        out.print(propertyString);
        return Iterables.count(pc.getPropertyKeys());
    }

    private String formatPropertyName(String prop) {
        return ", `" + prop + "`:";
    }

    private String formatProperties(PropertyContainer pc, Long id) {
        StringBuilder result = new StringBuilder(1000);
        List<String> keys = Iterables.asList(pc.getPropertyKeys());
        Collections.sort(keys);
        for (String prop : keys) {
            if (!indexedProperties.contains(prop)) continue;
            result.append(formatPropertyName(prop))
                    .append(toString(pc.getProperty(prop)));
        }
        for (String prop : keys) {
            if (indexedProperties.contains(prop)) continue;
            result.append(formatPropertyName(prop))
                    .append(toString(pc.getProperty(prop)));
        }
        if (id != null) {
            result.append(formatPropertyName(UNIQUE_ID_PROP)).append(id);
        }
        return "{" + result.substring(2) + "}";
    }

    private String toString(Iterator<?> iterator) {
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            if (result.length() > 0) result.append(", ");
            Object value = iterator.next();
            result.append(toString(value));
        }
        return "[" + result + "]";
    }

    private String arrayToString(Object value) {
        int length = Array.getLength(value);
        StringBuilder result = new StringBuilder(10 * length);
        for (int i = 0; i < length; i++) {
            if (i > 0) result.append(", ");
            result.append(toString(Array.get(value, i)));
        }
        return "[" + result.toString() + "]";
    }

    private String toString(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return FormatUtils.formatString(value);
        if (value instanceof Number) {
            return FormatUtils.formatNumber((Number) value);
        }
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Iterator) {
            return toString(((Iterator) value));
        }
        if (value instanceof Iterable) {
            return toString(((Iterable) value).iterator());
        }
        if (value.getClass().isArray()) {
            return arrayToString(value);
        }
        return value.toString();
    }
}
