package apoc.export.cypher.formatter;

import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
		if (node.getPropertyKeys().iterator().hasNext()) {
			result.append(" {");
			result.append(CypherFormatterUtils.formatNodeProperties("", node, uniqueConstraints, indexNames, true));
			result.append("}");
		}
		result.append(");");
		return result.toString();
	}

	@Override
    public String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties) {
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
    public void statementForSameNodes(Iterable<Node> nodes, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter) {
        // Map<Labels, NodeList>
        Map<String, List<Node>> map = new HashMap<>();

        // Map<Labels, Ids>
        Map<String, Set<String>> mapIds = new HashMap<>();

        int batchSize = exportConfig.getBatchSize();

        AtomicInteger nodeCount = new AtomicInteger(0);
        nodes.forEach(node -> {
            nodeCount.incrementAndGet();
            String key = CypherFormatterUtils.formatAllLabels(node, uniqueConstraints, indexNames);
            map.compute(key, (k, v) -> {
                if (v == null) {
                    v = new ArrayList<>();
                }
                v.add(node);
                return v;
            });
            mapIds.put(key, CypherFormatterUtils.getNodeIdProperties(node, uniqueConstraints).keySet());
        });

        AtomicInteger propertiesCount = new AtomicInteger(0);
        AtomicInteger batchCountBegin = new AtomicInteger(0);
        AtomicInteger batchCountCommit = new AtomicInteger(0);

        int unwindBatchSize = exportConfig.getUnwindBatchSize();

        map.forEach((labels, nodeList) -> {
            boolean begin = batchCountBegin.getAndAdd(nodeList.size()) % batchSize == 0;
            boolean commit = batchCountCommit.addAndGet(nodeList.size()) % batchSize == 0;

            if (begin) {
                out.append(exportConfig.getFormat().begin());
            }

            int nodeListSize = nodeList.size();
            for (int i = 0; i < nodeListSize; i++) {
                if (i % unwindBatchSize == 0) {
                    out.append("UNWIND [");
                }

                Node node = nodeList.get(i);

                Map<String, Object> props = node.getAllProperties();
                // start element
                out.append("{");

                // id
                out.append(CypherFormatterUtils.getNodeIdProperties(node, uniqueConstraints).entrySet().stream()
                        .map(e -> String.format("`%s`: %s", e.getKey(), CypherFormatterUtils.toString(e.getValue()))).collect(Collectors.joining(",")));

                // properties
                out.append(", ");
                out.append("properties: ");
                out.append("{");
                if (!props.isEmpty()) {
                    out.append(CypherFormatterUtils.formatProperties("", props, true).substring(2));
                    propertiesCount.addAndGet(props.size());
                }
                out.append("}");

                // end element
                out.append("}");
                boolean isEnd = i == nodeListSize - 1;
                if (isEnd || (i + 1) % unwindBatchSize == 0) {
                    out.append("] as row ");
                    out.append(StringUtils.LF);
                    out.append("MERGE ");
                    out.append(String.format("(n%s{", labels));
                    out.append(mapIds.get(labels).stream().map(s -> String.format("`%s`: row.`%s`", s, s)).collect(Collectors.joining(",")));
                    out.append("}) SET n += row.properties;");
                    out.append(StringUtils.LF);
                } else {
                    out.append(", ");
                }
            }
            if (commit) {
                out.append(exportConfig.getFormat().commit());
            }
        });

        if (batchCountCommit.get() % batchSize != 0) {
            out.append(exportConfig.getFormat().commit());
        }

        reporter.update(nodeCount.get(), 0, propertiesCount.longValue());
    }

    @Override
    public void statementForSameRelationship(Iterable<Relationship> relationship, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter) {
        // Map<Path(map{start,rel,end}),RelList>
        Map<Map<String, String>, List<Relationship>> map = new HashMap<>();

        // Map<Labels, Ids>
        Map<String, Set<String>> mapIds = new HashMap<>();

        int batchSize = exportConfig.getBatchSize();

        AtomicInteger relCount = new AtomicInteger(0);

        relationship.forEach(rel -> {
            relCount.incrementAndGet();
            // define the start labels
            Node start = rel.getStartNode();
            String startLabels = CypherFormatterUtils.formatAllLabels(start, uniqueConstraints, indexNames);
            mapIds.put(startLabels, CypherFormatterUtils.getNodeIdProperties(start, uniqueConstraints).keySet());

            // define the end labels
            Node end = rel.getEndNode();
            String endLabels = CypherFormatterUtils.formatAllLabels(end, uniqueConstraints, indexNames);
            mapIds.put(endLabels, CypherFormatterUtils.getNodeIdProperties(end, uniqueConstraints).keySet());

            // define the type
            String type = rel.getType().name();

            // create the path
            Map<String,String> key = new HashMap<>();
            key.put("start", startLabels);
            key.put("type", type);
            key.put("end", endLabels);

            map.compute(key, (k, v) -> {
                if (v == null) {
                    v = new ArrayList<>();
                }
                v.add(rel);
                return v;
            });
        });

        AtomicInteger propertiesCount = new AtomicInteger(0);
        AtomicInteger batchCountBegin = new AtomicInteger(0);
        AtomicInteger batchCountCommit = new AtomicInteger(0);

        int unwindBatchSize = exportConfig.getUnwindBatchSize();

        map.forEach((path, relationshipList) -> {
            boolean begin = batchCountBegin.getAndAdd(relationshipList.size()) % batchSize == 0;
            boolean commit = batchCountCommit.addAndGet(relationshipList.size()) % batchSize == 0;

            if (begin) {
                out.append(exportConfig.getFormat().begin());
            }

            for (int i = 0; i < relationshipList.size(); i++) {
                if (i % unwindBatchSize == 0) {
                    out.append("UNWIND [");
                }
                Relationship rel = relationshipList.get(i);

                Map<String, Object> props = rel.getAllProperties();
                // start element
                out.append("{");

                // start node
                out.append("start: ");
                out.append("{");
                out.append(CypherFormatterUtils.getNodeIdProperties(rel.getStartNode(), uniqueConstraints).entrySet().stream()
                        .map(e -> String.format("`%s`: %s", e.getKey(), CypherFormatterUtils.toString(e.getValue()))).collect(Collectors.joining(",")));
                out.append("}");

                out.append(", ");

                // end node
                out.append("end: ");
                out.append("{");

                out.append(CypherFormatterUtils.getNodeIdProperties(rel.getEndNode(), uniqueConstraints).entrySet().stream()
                        .map(e -> String.format("`%s`: %s", e.getKey(), CypherFormatterUtils.toString(e.getValue()))).collect(Collectors.joining(",")));
                out.append("}");

                // properties
                out.append(", ");
                out.append("properties: ");
                out.append("{");
                if (!props.isEmpty()) {
                    out.append(CypherFormatterUtils.formatProperties("", props, true).substring(2));
                    propertiesCount.addAndGet(props.size());
                }
                out.append("}");

                // end element
                out.append("}");

                boolean isEnd = i == relationshipList.size() - 1;
                if (isEnd || (i + 1) % unwindBatchSize == 0) {
                    out.append("] as row ");

                    out.append(StringUtils.LF);
                    out.append("MATCH ");

                    // match start node
                    out.append(String.format("(start%s{", path.get("start")));
                    out.append(mapIds.get(path.get("start")).stream().map(s -> String.format("`%s`: row.start.`%s`", s, s)).collect(Collectors.joining(",")));
                    out.append("})");

                    out.append(", ");

                    // match end node
                    out.append(String.format("(end%s{", path.get("end")));
                    out.append(mapIds.get(path.get("end")).stream().map(s -> String.format("`%s`: row.end.`%s`", s, s)).collect(Collectors.joining(",")));
                    out.append("})");

                    out.append(StringUtils.LF);

                    // merge relationship
                    out.append(String.format("MERGE (start)-[r:`%s`]->(end) SET r += row.properties;", path.get("type")));
                    out.append(StringUtils.LF);
                } else {
                    out.append(", ");
                }
            }
            if (commit) {
                out.append(exportConfig.getFormat().commit());
            }
        });

        if (batchCountCommit.get() % batchSize != 0) {
            out.append(exportConfig.getFormat().commit());
        }
        reporter.update(0, relCount.get(), propertiesCount.longValue());
    }
}