package apoc.diff;

import apoc.Description;
import apoc.export.util.FormatUtils;
import apoc.export.util.MapSubGraph;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.map;

/**
 * @author Benjamin Clauss
 * @since 15.06.2018
 */
public class Diff {

    public static final String NODE = "Node";
    public static final String RELATIONSHIP = "Relationship";
    public static final String DESTINATION_ENTITY_NOT_FOUND = "Destination Entity not found";
    @Context
    public GraphDatabaseService db;

    @UserFunction()
    @Description("apoc.diff.nodes([leftNode],[rightNode]) returns a detailed diff of both nodes")
    public Map<String, Object> nodes(@Name("leftNode") Node leftNode, @Name("rightNode") Node rightNode) {
        Map<String, Object> allLeftProperties = leftNode.getAllProperties();
        Map<String, Object> allRightProperties = rightNode.getAllProperties();

        Map<String, Object> result = new HashMap<>();
        result.put("leftOnly", getPropertiesOnlyLeft(allLeftProperties, allRightProperties));
        result.put("rightOnly", getPropertiesOnlyLeft(allRightProperties, allLeftProperties));
        result.put("inCommon", getPropertiesInCommon(allLeftProperties, allRightProperties));
        result.put("different", getPropertiesDiffering(allLeftProperties, allRightProperties));

        return result;
    }

    private Map<String, Object> getPropertiesOnlyLeft(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> leftOnly = new HashMap<>();
        leftOnly.putAll(left);
        leftOnly.keySet().removeAll(right.keySet());
        return leftOnly;
    }

    private Map<String, Object> getPropertiesInCommon(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> inCommon = new HashMap<>(left);
        inCommon.entrySet().retainAll(right.entrySet());
        return inCommon;
    }

    private Map<String, Map<String, Object>> getPropertiesDiffering(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Map<String, Object>> different = new HashMap<>();
        Map<String, Object> keyPairs = new HashMap<>();
        keyPairs.putAll(left);
        keyPairs.keySet().retainAll(right.keySet());

        for (Map.Entry<String, Object> entry : keyPairs.entrySet()) {
            if (!left.get(entry.getKey()).equals(right.get(entry.getKey()))) {
                Map<String, Object> pairs = new HashMap<>();
                pairs.put("left", left.get(entry.getKey()));
                pairs.put("right", right.get(entry.getKey()));
                different.put(entry.getKey(), pairs);
            }
        }
        return different;
    }

    public static class SourceDestResult {
        public final String difference;
        public final String entityType;
        public final Long id;
        public final String sourceLabel;
        public final String destLabel;
        public final Object source;
        public final Object dest;

        public SourceDestResult(String difference, String entityType, Long id, String sourceLabel,
                                String destLabel, Object source, Object dest) {
            this.difference = difference;
            this.entityType = entityType;
            this.id = id;
            this.sourceLabel = sourceLabel;
            this.destLabel = destLabel;
            this.source = source;
            this.dest = dest;
        }

        public SourceDestResult(String difference, String entityType, Object source, Object dest) {
            this.difference = difference;
            this.entityType = entityType;
            this.id = null;
            this.sourceLabel = null;
            this.destLabel = null;
            this.source = source;
            this.dest = dest;
        }

        private boolean areSourceAndDestEqual() {
            if (source == null && dest == null) return true;
            if (source == null || dest == null) return false;
            return source.equals(dest);
        }
    }

    @Procedure("apoc.diff.graphs")
    @Description("CALL apoc.diff.nodes(<source>, <dest>, <config>) YIELD difference, entityType, id, sourceLabel, destLabel, source, dest - compares two graphs and returns the results")
    public Stream<SourceDestResult> compare(@Name(value = "source") Object source,
                                        @Name(value = "dest") Object dest,
                                        @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        config = config == null ? Collections.emptyMap() : config;
        SubGraph sourceGraph = toSubGraph(source, config, SourceDestConfig.fromMap((Map<String, Object>) config.get("source")));
        SubGraph destGraph = toSubGraph(dest, config, SourceDestConfig.fromMap((Map<String, Object>) config.get("dest")));

        Function<Map<String, Long>, Long> sum = (map) -> map.values().stream().reduce(0L, (x, y) -> x + y);
        final SourceDestResult labelNodeCount = sourceDestCountByLabel(sourceGraph, destGraph);
        final SourceDestResult nodeCount = labelNodeCount.areSourceAndDestEqual() ?
                null : new SourceDestResult("Total count", NODE,
                    sum.apply((Map<String, Long>) labelNodeCount.source), sum.apply((Map<String, Long>) labelNodeCount.dest));
        final SourceDestResult typeRelCount = sourceDestCountByType(sourceGraph, destGraph);
        final SourceDestResult relCount = typeRelCount.areSourceAndDestEqual() ?
                null : new SourceDestResult("Total count", RELATIONSHIP,
                    sum.apply((Map<String, Long>) typeRelCount.source), sum.apply((Map<String, Long>) typeRelCount.dest));

        final Stream<SourceDestResult> generalStream = Stream.of(
                nodeCount, nodeCount != null ? labelNodeCount : null,
                relCount, relCount != null ? typeRelCount : null)
                .filter(elem -> elem != null);
        DiffConfig diffConfig = new DiffConfig(config);
        final Stream<SourceDestResult> nodeStream = compareNodes(sourceGraph, destGraph, diffConfig);
        final Stream<SourceDestResult> relStream = compareRels(sourceGraph, destGraph);
        return Stream.of(generalStream, nodeStream, relStream)
                .reduce(Stream::concat)
                .orElse(Stream.empty());
    }

    private SubGraph toSubGraph(Object input, Map<String, Object> config, SourceDestConfig sourceDestConfig) {
        if (input == null) {
            throw new NullPointerException("Input data is null");
        }
        if (input instanceof Map) {
            Map<String, Object> graph = (Map<String, Object>) input;
            if (graph.containsKey("schema")) {
                return new MapSubGraph(graph);
            } else {
                Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
                Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
                return new NodesAndRelsSubGraph(db, nodes, rels);
            }
        }
        if (input instanceof String) {
            final String inputString = (String) input;
            if (sourceDestConfig != null) {
                if (StringUtils.isNotBlank(sourceDestConfig.getTarget().getValue())) {
                    switch (sourceDestConfig.getTarget().getType()) {
                        case URL:
                            final Map<String, List<Object>> graph = createMapFromRemoteDb(inputString,
                                    sourceDestConfig.getTarget().getValue(),
                                    sourceDestConfig.getParams());
                            return toSubGraph(graph, config, null);
                        default:
                            throw new IllegalArgumentException("The following type is not supported: " + sourceDestConfig.getTarget().getType());
                    }
                } else {
                    return toSubGraph(db.execute(inputString, sourceDestConfig.getParams()), config, null);
                }
            }
            return toSubGraph(db.execute(inputString), config, null);
        }
        if (input instanceof Result) {
            Result result = (Result) input;
            return CypherResultSubGraph.from(result, db, Util.toBoolean(config.getOrDefault("relsInBetween", false)));
        }
        if (input instanceof Path) {
            Path path = (Path) input;
            return new NodesAndRelsSubGraph(db, Iterables.asCollection(path.nodes()),
                    Iterables.asCollection(path.relationships()));
        }
        throw new IllegalArgumentException("Unsupported input type: " + input.getClass().getName());
    }

    private Map<String, List<Object>> createMapFromRemoteDb(String inputString, String url, Map<String, Object> params) {
        params = params == null ? Collections.emptyMap() : params;
        String boltLoadQuery = "CALL apoc.bolt.load($url, $boltQuery, $params, $boltConfig) YIELD row";
        final Map<String, Object> boltConfig = map("virtual", true, "withRelationshipNodeProperties", true);

        final Result execute = db.execute(boltLoadQuery, map("boltConfig", boltConfig, "boltQuery", inputString, "url", url, "params", params));

        final Map<String, List<Object>> graph = createBaseMapFromRemoteDb(execute);

        final Optional<List<Object>> schemaOpt = retrieveSchemaFromRemoteDB(boltLoadQuery, boltConfig, url);
        schemaOpt.ifPresent((schema) -> graph.put("schema", schema));
        return graph;
    }

    private Optional<List<Object>> retrieveSchemaFromRemoteDB(String boltLoadQuery,
                                                              Map<String, Object> boltConfig,
                                                              String url) {
        String boltQuery = "CALL db.indexes() YIELD tokenNames, properties, state, type\n" +
                "WHERE state = 'ONLINE' AND type = 'node_unique_property'\n" +
                "RETURN collect({labels: tokenNames, properties: properties, type: type}) AS schema\n";
        return db.execute(boltLoadQuery, map("boltConfig", boltConfig, "boltQuery", boltQuery, "url", url, "params", Collections.emptyMap()))
                .stream()
                .map(row -> (Map<String, Object>) row.get("row"))
                .map(row -> (List<Object>) row.get("schema"))
                .findFirst();
    }

    private Map<String, List<Object>> createBaseMapFromRemoteDb(Result execute) {
        return execute.stream()
                .map(row -> row.get("row"))
                .map(this::extractGraphEntity)
                .flatMap(elem -> elem instanceof Collection ? ((Collection<Object>) elem).stream() : Stream.of(elem))
                .map(value -> {
                    final String key;
                    if (value instanceof Node) {
                        key = "nodes";
                    } else {
                        key = "relationships";
                    }
                    return new AbstractMap.SimpleEntry<>(key, value);
                })
                .collect(Collectors.groupingBy(e -> e.getKey(), Collectors.mapping(e -> e.getValue(), Collectors.toList())));
    }

    private Object extractGraphEntity(Object input) {
        if (input instanceof Collection) {
            return ((Collection) input).stream()
                    .flatMap(elem -> elem instanceof Collection ? ((Collection<Object>) elem).stream() : Stream.of(elem))
                    .map(this::extractGraphEntity)
                    .collect(Collectors.toList());
        }
        if (input instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>) input;
            return extractGraphEntity(map.values());
        }
        if (input instanceof Node || input instanceof Relationship) {
            return input;
        }
        throw new RuntimeException("Type not managed: " + input.getClass().getSimpleName());
    }

    private Map<String, Long> countByLabel(SubGraph graph) {
        return StreamSupport.stream(graph.getNodes().spliterator(), false)
                .flatMap(n -> StreamSupport.stream(n.getLabels().spliterator(), false)
                        .map(Label::name))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private Map<String, Long> countByType(SubGraph graph) {
        return StreamSupport.stream(graph.getRelationships().spliterator(), false)
                .map(r -> r.getType().name())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private SourceDestResult sourceDestCountByLabel(SubGraph source, SubGraph dest) {
        return new SourceDestResult("Count by Label", NODE, countByLabel(source), countByLabel(dest));
    }

    private SourceDestResult sourceDestCountByType(SubGraph source, SubGraph dest) {
        return new SourceDestResult("Count by Type", RELATIONSHIP, countByType(source), countByType(dest));
    }

    private <T extends Entity> T findEntityById(Iterable<T> it, long id) {
        return StreamSupport.stream(it.spliterator(), true)
                .filter(entity -> entity.getId() == id)
                .findFirst()
                .orElse(null);
    }

    private Node findNode(Iterable<Node> it, Node node, SubGraph graph, DiffConfig config) {
        ConstraintDefinition constraintDefinition = getConstraint(node, graph);
        if (constraintDefinition == null) {
            return config.isFindById() ? findEntityById(it, node.getId()) : null;
        }
        Map<String, Object> keys = getNodeKeys(node, constraintDefinition);
        return StreamSupport.stream(it.spliterator(), true)
                .filter(entity -> entity.getProperties(Iterables.asArray(String.class, keys.keySet())).equals(keys))
                .findFirst()
                .orElse(null);
    }

    private ConstraintDefinition getConstraint(Node node, SubGraph graph) {
        ConstraintDefinition constraintDefinition = null;
        for (Label label : node.getLabels()) {
            for (ConstraintDefinition constr : graph.getConstraints()) {
                if (!constr.getLabel().name().equals(label.name())) continue;
                final long count = Iterables.count(constr.getPropertyKeys());
                if (constraintDefinition == null || Iterables.count(constraintDefinition.getPropertyKeys()) > count) {
                    constraintDefinition = constr;
                    if (count == 1) {
                        break;
                    }
                }
            }
        }
        return constraintDefinition;
    }

    private SourceDestDTO sourceDestMap(Object sourceVal, Object destVal) {
        return new SourceDestDTO(sourceVal, destVal);
    }

    private SourceDestDTO transformDiff(Map<String, Map<String, Object>> propDiffs) {
        Map<String, Object> sourceFields = new HashMap<>();
        Map<String, Object> destFields = new HashMap<>();
        propDiffs.forEach((prop, diff) -> {
            sourceFields.put(prop, diff.get("left"));
            destFields.put(prop, diff.get("right"));
        });
        return sourceDestMap(sourceFields, destFields);
    }

    private class SourceDestDTO {
        private final Object source;
        private final Object dest;
        SourceDestDTO(Object source, Object dest) {
            this.source = source;
            this.dest = dest;
        }
    }

    private Stream<SourceDestResult> compareRels(SubGraph sourceGraph, SubGraph destGraph) {
        return StreamSupport.stream(sourceGraph.getRelationships().spliterator(), true)
                .map(sourceRel -> {
                    final Map<String, Object> startKeys = getNodeKeys(sourceRel.getStartNode(), sourceGraph);
                    final Map<String, Object> endKeys = getNodeKeys(sourceRel.getEndNode(), sourceGraph);
                    final Map<String, Object> sourceRelAllProperties = sourceRel.getAllProperties();
                    final Relationship destRel = StreamSupport.stream(destGraph.getRelationships().spliterator(), true)
                            .filter(elem -> {
                                final Map<String, Object> startDestKeys = getNodeKeys(elem.getStartNode(), destGraph);
                                final Map<String, Object> endDestKeys = getNodeKeys(elem.getEndNode(), destGraph);
                                boolean areKeysEqual = startKeys.equals(startDestKeys) && endKeys.equals(endDestKeys);
                                if (!sourceRelAllProperties.isEmpty()) {
                                    return areKeysEqual && sourceRelAllProperties.equals(elem.getAllProperties());
                                } else {
                                    return areKeysEqual;
                                }
                            })
                            .findFirst()
                            .orElse(null);
                    return destRel == null ? new SourceDestResult(DESTINATION_ENTITY_NOT_FOUND,
                            RELATIONSHIP, sourceRel.getId(), sourceRel.getType().name(), null,
                            Util.map("start", startKeys, "end", endKeys, "properties", sourceRelAllProperties), null) : null;
                })
                .filter(entity -> entity != null);
    }

    private Map<String, Object> getNodeKeys(Node node, ConstraintDefinition constraint) {
        if (constraint == null) return null;
        String[] propKeys = Iterables.asList(constraint.getPropertyKeys()).toArray(new String[0]);
        return node.getProperties(propKeys);
    }

    private Map<String, Object> getNodeKeys(Node node, SubGraph subGraph) {
        ConstraintDefinition constraint = getConstraint(node, subGraph);
        return getNodeKeys(node, constraint);
    }

    private Stream<SourceDestResult> compareNodes(SubGraph source, SubGraph dest, DiffConfig config) {
        return StreamSupport.stream(source.getNodes().spliterator(), true)
               .map(node -> new AbstractMap.SimpleEntry<>(node, findNode(dest.getNodes(), node, dest, config)))
               .flatMap(entry -> {
                   List<SourceDestResult> diffs = new ArrayList<>();
                   final Node sourceNode = entry.getKey();
                   final Node destNode = entry.getValue();

                   final String sourceLabel = getFirstLabel(sourceNode);
                   final long id = sourceNode.getId();

                   if (destNode == null) {
                       final Map<String, Object> nodeKeys = getNodeKeys(sourceNode, getConstraint(sourceNode, source));
                       diffs.add(new SourceDestResult(DESTINATION_ENTITY_NOT_FOUND, NODE, id, sourceLabel, null, nodeKeys, null));
                   } else {
                       final String destLabel = getFirstLabel(destNode);
                       List<String> sourceLabels = FormatUtils.getLabelsSorted(sourceNode);
                       List<String> destLabels = FormatUtils.getLabelsSorted(destNode);
                       if (!sourceLabels.equals(destLabels)) {
                           diffs.add(new SourceDestResult("Different Labels", NODE, id, sourceLabel, destLabel, sourceLabels, destLabels));
                       } else {
                           final Map<String, Map<String, Object>> propDiff = getPropertiesDiffering(sourceNode.getAllProperties(),
                                   destNode.getAllProperties());
                           if (!propDiff.isEmpty()) {
                               final SourceDestDTO sourceDestDTO = transformDiff(propDiff);
                               diffs.add(new SourceDestResult("Different Properties", NODE, id, sourceLabel, destLabel, sourceDestDTO.source, sourceDestDTO.dest));
                           } else { // if the two nodes are equal lets compare the relationships
                               return diffs.stream();
                           }
                       }
                   }
                   return diffs.stream();
               });
    }

    private String getFirstLabel(Node sourceNode) {
        return StreamSupport.stream(sourceNode.getLabels().spliterator(), false)
                .map(Label::name)
                .findFirst()
                .orElse(null);
    }
}
