package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import apoc.result.VirtualGraph;
import apoc.result.VirtualNode;
import apoc.util.FixedSizeStringWriter;
import apoc.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DocumentToGraph {

    private GraphDatabaseService db;
    private RelationshipBuilder documentRelationBuilder;
    private LabelBuilder documentLabelBuilder;
    private GraphsConfig config;

    public DocumentToGraph(GraphDatabaseService db, GraphsConfig config) {
        this.db = db;
        this.documentRelationBuilder = new RelationshipBuilder(config);
        this.documentLabelBuilder = new LabelBuilder(config);
        this.config = config;
    }

    public Map<Map<String, Object>, List<String>> validate(Map<String, Object> map) {
        return flatMapFields(map)
                .map(elem -> {
                    List<String> msgs = new ArrayList<>();
                    if (!elem.containsKey(config.getIdField())) {
                        msgs.add("`" + config.getIdField() + "` as id-field name");
                    }
                    if (!elem.containsKey(config.getLabelField())) {
                        msgs.add("`" + config.getLabelField() + "` as label-field name");
                    }
                    return new AbstractMap.SimpleEntry<>(elem, msgs);
                })
                .filter(elem -> !elem.getValue().isEmpty())
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    public String formatDocument(Map map) {
        try (FixedSizeStringWriter writer = new FixedSizeStringWriter(100)) {
            JsonUtil.OBJECT_MAPPER.writeValue(writer, map);
            return writer.toString().concat(writer.isExceeded() ? "...}" : "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void fromDocument(Map<String, Object> document, Node source, String type,
                              Map<String, Set<Node>> nodes, Set<Relationship> relationships) {
        boolean isRootNode = source == null;
        prepareData(document);
        Map<Map<String, Object>, List<String>> errors = validate(document);
        if (!errors.isEmpty()) {
            throwError(errors);
        }
        Label label = this.documentLabelBuilder.buildLabel(document);

        Object idValue = document.get(config.getIdField());
        // retrieve the current node
        final Node node;
        if (this.config.isWrite()) {
            node = getOrCreateRealNode(label, idValue);
        } else {
            node = getOrCreateVirtualNode(nodes, label, idValue);
        }

        // write node properties
        document.entrySet().stream()
                .filter(e -> isSimpleType(e))
                .forEach(e -> {
                    Object value = e.getValue();
                    if (value instanceof List) {
                        List list = (List) value;
                        if (!list.isEmpty()) {
                            value = Array.newInstance(list.get(0).getClass(), list.size());
                            for (int i = 0; i < list.size(); i++) {
                                Array.set(value, i, list.get(i));
                            }
                        }
                    }
                    node.setProperty(e.getKey(), value);
                });

        // get child nodes
        document.entrySet().stream()
                .filter(e -> !isSimpleType(e))
                .forEach(e -> {
                    if (e.getValue() instanceof Map) { // if value is a complex object (map)
                        Map inner = (Map) e.getValue();
                        fromDocument(inner, node, e.getKey(), nodes, relationships);
                    } else {
                        List<Map> list = (List) e.getValue(); // if value is and array
                        list.forEach(map -> fromDocument(map, node, e.getKey(), nodes, relationships));
                    }
                });

        Set<Node> nodesWithSameIds = getNodesWithSameIds(nodes, idValue);
        nodesWithSameIds.add(node);
        if (!isRootNode) {
            relationships.addAll(documentRelationBuilder.buildRelation(source, node, type));
        }

    }

    private void throwError(Map<Map<String, Object>, List<String>> errors) {
        String error = formatError(errors);
        throw new RuntimeException(error);
    }

    private String formatError(Map<Map<String, Object>, List<String>> errors) {
        return errors.entrySet().stream()
                .map(e -> "The object `" + formatDocument(e.getKey()) + "` must have " + String.join(" and ", e.getValue()))
                .collect(Collectors.joining(StringUtils.LF));
    }

    public void prepareData(Map<String, Object> document) {
        if (config.isGenerateId()) {
            document.computeIfAbsent(config.getIdField(), key -> UUID.randomUUID().toString());
        }
        if (!config.getDefaultLabel().isEmpty()) {
            document.computeIfAbsent(config.getLabelField(), key -> config.getDefaultLabel());
        }
    }

    private Set<Node> getNodesWithSameIds(Map<String, Set<Node>> nodes, Object idValue) {
        return nodes.computeIfAbsent(idValue.toString(), (k) -> new LinkedHashSet<>());
    }

    private Node getOrCreateVirtualNode(Map<String, Set<Node>> nodes, Label label, Object idValue) {
        Set<Node> nodesWithSameIds = getNodesWithSameIds(nodes, idValue);
        return nodesWithSameIds
                .stream()
                .filter(n -> {
                    if (n.hasLabel(label)) {
                        return true;
                    }
                    return StreamSupport.stream(n.getRelationships().spliterator(), false)
                            .anyMatch(r -> r.getOtherNode(n).hasLabel(label));
                })
                .findFirst()
                .orElse(new VirtualNode(new Label[]{label}, Collections.emptyMap(), db));
    }

    private Node getOrCreateRealNode(Label label, Object idValue) {
        Node nodeInDB = db.findNode(label, config.getIdField(), idValue);
        return nodeInDB != null ? nodeInDB : db.createNode(label);
    }

    private boolean isSimpleType(Map.Entry<String, Object> e) {
        if (e.getValue() instanceof Map) {
            return false;
        }
        if (e.getValue() instanceof List) {
            List list = (List) e.getValue();
            if (!list.isEmpty()) {
                Object object = list.get(0); // assumption: homogeneous array
                if (object instanceof Map) { // if is an array of complex type
                    return false;
                }
            }
        }
        return true;
    }

    private List<Map<String, Object>> getDocumentCollection(Object document) {
        List<Map<String, Object>> coll;
        if (document instanceof String) {
            document  = JsonUtil.parse((String) document, null, Object.class);
        }
        if (document instanceof List) {
            coll = (List) document;
        } else {
            coll = Arrays.asList((Map) document);
        }
        return coll;
    }

    public VirtualGraph create(Object documentObj) {
        Collection<Map<String, Object>> coll = getDocumentCollection(documentObj);
        Map<String, Set<Node>> nodes = new LinkedHashMap<>();
        Set<Relationship> relationships = new LinkedHashSet<>();
        coll.forEach(map -> fromDocument(map, null, null, nodes, relationships));
        return new VirtualGraph("Graph", nodes.values().stream().flatMap(Set::stream).collect(Collectors.toCollection(LinkedHashSet::new)), relationships, Collections.emptyMap());
    }

    public Map<Long, List<String>> findDuplicates(Object doc) {
        // duplicate validation
        // the check on duplicates must be provided on raw data without apply the default label or auto generate the id
        AtomicLong index = new AtomicLong(-1);
        return getDocumentCollection(doc).stream()
                .flatMap(e -> {
                    long lineDup = index.incrementAndGet();
                    return flatMapFields(e)
                            .map(ee -> new AbstractMap.SimpleEntry<Map, Long>(ee, lineDup));
                })
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet()
                .stream()
                .filter(e -> e.getValue().size() > 1)
                .map(e -> {
                    long line = e.getValue().get(0);
                    String elem = formatDocument(e.getKey());
                    String dupLines = e.getValue().subList(1, e.getValue().size())
                            .stream()
                            .map(ee -> String.valueOf(ee))
                            .collect(Collectors.joining(","));
                    return new AbstractMap.SimpleEntry<>(line,
                            String.format("The object `%s` has duplicate at lines [%s]", elem, dupLines));
                })
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
    }

    private Stream<Map<String, Object>> flatMapFields(Map<String, Object> map) {
        Stream<Map<String, Object>> stream = Stream.of(map);
        return Stream.concat(stream, map.values()
                .stream()
                .filter(e -> e instanceof Map)
                .flatMap(e -> flatMapFields((Map<String, Object>) e)));
    }

    public Map<Long, String> validate(Object doc) {
        AtomicLong index = new AtomicLong(-1);
        return getDocumentCollection(doc).stream()
                .map(elem -> {
                    long line = index.incrementAndGet();
                    prepareData(elem);
                    // id, label validation
                    Map<Map<String, Object>, List<String>> errors = validate(elem);
                    if (errors.isEmpty()) {
                        return null;
                    } else {
                        return new AbstractMap.SimpleEntry<>(line, formatError(errors));
                    }
                })
                .filter(e -> e != null)
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    public Map<Long, List<String>> validateDocument(Object document) {
        Map<Long, List<String>> dups = findDuplicates(document);
        Map<Long, String> invalids = validate(document);

        for (Map.Entry<Long, String> invalid : invalids.entrySet()) {
            dups.computeIfAbsent(invalid.getKey(), (key) -> new ArrayList<>()).add(invalid.getValue());
        }
        return dups;
    }
}