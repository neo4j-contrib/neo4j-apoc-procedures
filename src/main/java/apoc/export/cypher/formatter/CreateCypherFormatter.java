package apoc.export.cypher.formatter;

import org.neo4j.helpers.collection.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
    public Map<String,Object> statementForSameNodes(Iterable<Node> nodes, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        Map<String,Object> resultMap = new HashMap<>();

        // Map<Labels, NodeList>
        Map<String, List<Node>> map = new HashMap<>();

        // Map<Labels, Ids>
        Map<String, Set<String>> mapIds = new HashMap<>();

        nodes.forEach(node -> {
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

        AtomicReference<Long> count = new AtomicReference<>(0L);
        AtomicInteger propertiesCount = new AtomicInteger();
        map.forEach((labels, nodeList) -> {
            result.append("UNWIND [");

            int nodeListSize = nodeList.size();
            for (int i = 0; i < nodeListSize; i++) {
                Node node = nodeList.get(i);

                Map<String, Object> props = node.getAllProperties();
                // start element
                result.append("{");

                // id
                result.append(CypherFormatterUtils.getNodeIdProperties(node, uniqueConstraints).entrySet().stream()
                        .map(e -> String.format("`%s`: %s", e.getKey(), CypherFormatterUtils.toString(e.getValue()))).collect(Collectors.joining(",")));


                // properties
                result.append(", ");
                result.append("properties: ");
                result.append("{");
                if (!props.isEmpty()) {
                    result.append(CypherFormatterUtils.formatProperties("", props, true).substring(2));
                }
                result.append("}");

                // end element
                result.append("}");
                if (i != nodeListSize - 1) {
                    result.append(", ");
                }
                propertiesCount.addAndGet(props.size());
            }

            result.append("] as row ");

            result.append(StringUtils.LF);
            result.append("MERGE ");
            result.append(String.format("(n%s{", labels));
            result.append(mapIds.get(labels).stream().map(s -> String.format("`%s`: row.`%s`", s, s)).collect(Collectors.joining(",")));
            result.append("}) SET n += row.properties;");
            result.append(StringUtils.LF);
//            result.append(StringUtils.LF);
//            result.append(String.format("WITH n%d ", count.get()));
            count.getAndSet(count.get() + 1);
        });

        resultMap.put("statement", result.toString());
        resultMap.put("properties", propertiesCount.longValue());

        return resultMap;
    }

    @Override
    public Map<String,Object> statementForSameRelationship(Iterable<Relationship> relationship, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
        StringBuilder result = new StringBuilder(100);
        Map<String,Object> resultMap = new HashMap<>();

        // Map<Path(map{start,rel,end}),RelList>
        Map<Map<String, String>, List<Relationship>> map = new HashMap<>();

        // Map<Labels, Ids>
        Map<String, Set<String>> mapIds = new HashMap<>();


        relationship.forEach(rel -> {
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

        AtomicReference<Long> count = new AtomicReference<>(0L);
        AtomicInteger propertiesCount = new AtomicInteger();
        map.forEach((path, relationshipList) -> {
            result.append("UNWIND [");

            for (int i = 0; i < relationshipList.size(); i++) {
                Relationship rel = relationshipList.get(i);

                Map<String, Object> props = rel.getAllProperties();
                // start element
                result.append("{");

                // start node
                result.append("start: ");
                result.append("{");
                result.append(CypherFormatterUtils.getNodeIdProperties(rel.getStartNode(), uniqueConstraints).entrySet().stream()
                        .map(e -> String.format("`%s`: %s", e.getKey(), CypherFormatterUtils.toString(e.getValue()))).collect(Collectors.joining(",")));
                result.append("}");

                result.append(", ");

                // end node
                result.append("end: ");
                result.append("{");

                result.append(CypherFormatterUtils.getNodeIdProperties(rel.getEndNode(), uniqueConstraints).entrySet().stream()
                        .map(e -> String.format("`%s`: %s", e.getKey(), CypherFormatterUtils.toString(e.getValue()))).collect(Collectors.joining(",")));
                result.append("}");

                // properties
                result.append(", ");
                result.append("properties: ");
                result.append("{");
                if (!props.isEmpty()) {
                    result.append(CypherFormatterUtils.formatProperties("", props, true).substring(2));
                }
                result.append("}");

                // end element
                result.append("}");
                if (i != relationshipList.size() - 1) {
                    result.append(", ");
                }
                propertiesCount.addAndGet(props.size());
            }
            result.append("] as row ");

            result.append(StringUtils.LF);
            result.append("MATCH ");

            // match start node
            result.append(String.format("(start%s{", path.get("start")));
            result.append(mapIds.get(path.get("start")).stream().map(s -> String.format("`%s`: row.start.`%s`", s, s)).collect(Collectors.joining(",")));
            result.append("})");

            result.append(", ");

            // match end node
            result.append(String.format("(end%s{", path.get("end")));
            result.append(mapIds.get(path.get("end")).stream().map(s -> String.format("`%s`: row.end.`%s`", s, s)).collect(Collectors.joining(",")));
            result.append("})");

            result.append(StringUtils.LF);

            // merge relationship
            result.append(String.format("MERGE (start)-[r:`%s`]->(end) SET r += row.properties;", path.get("type")));
            result.append(StringUtils.LF);
//            result.append(String.format("WITH r%d ", count.get()));
            count.getAndSet(count.get() + 1);
        });

        resultMap.put("statement", result.toString());
        resultMap.put("properties", propertiesCount.longValue());

        return resultMap;

    }
}