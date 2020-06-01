package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import apoc.result.VirtualGraph;
import apoc.result.VirtualNode;
import apoc.util.FixedSizeStringWriter;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class DocumentToGraph {

    private static final String JSON_ROOT = "$";
    private final DocumentToNodes documentToNodes;

    private Transaction tx;
    private RelationshipBuilder documentRelationBuilder;
    private LabelBuilder documentLabelBuilder;
    private GraphsConfig config;

    public DocumentToGraph(Transaction tx, GraphsConfig config) {
       this(tx, config, new HashSet<>());
    }

    public DocumentToGraph(Transaction tx, GraphsConfig config, Set<Node> initialNodes) {
        this.tx = tx;
        this.documentRelationBuilder = new RelationshipBuilder(config);
        this.documentLabelBuilder = new LabelBuilder(config);
        this.config = config;
        this.documentToNodes = new DocumentToNodes(initialNodes, tx);
    }

    public <T> Set<T> toSet(Iterable<T> collection) {
        HashSet<T> set = new HashSet<T>();
        for (T item: collection)
            set.add(item);
        return set;
    }

    private boolean hasId(Map<String, Object> map, String path) {
        List<String> ids = config.idsForPath(path);
        if (ids.isEmpty()) {
            return map.containsKey(config.getIdField());
        } else {
            return map.keySet().containsAll(ids);
        }
    }

    private boolean hasLabel(Map<String, Object> map, String path) {
        return !config.labelsForPath(path).isEmpty() || map.containsKey(config.getLabelField());
    }

    public Map<Map<String, Object>, List<String>> validate(Map<String, Object> map, String path) {
        return flatMapFieldsWithPath(map, path)
                .entrySet()
                .stream()
                .flatMap(elem -> elem.getValue().stream().map(data -> new AbstractMap.SimpleEntry<>(elem.getKey(), data)))
                .map(elem -> {
                    String subPath = elem.getKey();
                    List<String> valueObjects = config.valueObjectForPath(subPath);
                    List<String> msgs = new ArrayList<>();
                    Map<String, Object> value = elem.getValue();
                    if (valueObjects.isEmpty()) {
                        if (!hasId(value, subPath)) {
                            msgs.add("`" + config.getIdField() + "` as id-field name");
                        }
                        if (!hasLabel(value, subPath)) {
                            msgs.add("`" + config.getLabelField() + "` as label-field name");
                        }
                    }
                    return new AbstractMap.SimpleEntry<>(value, msgs);
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
                              Map<Set<String>, Set<Node>> nodes, Set<Relationship> relationships,
                              String propertyName) {
        String path = propertyName == null ? JSON_ROOT : propertyName;

        // clean the object form unwanted properties
        if (!config.allPropertiesForPath(path)) {
            document.keySet().retainAll(config.propertiesForPath(path));
        }

        boolean isRootNode = source == null;
        prepareData(document, path);

        // validate
        if (!config.isSkipValidation()) {
            Map<Map<String, Object>, List<String>> errors = validate(document, path);
            if (!errors.isEmpty()) {
                throwError(errors);
            }
        }

        Label[] labels = this.documentLabelBuilder.buildLabel(document, path);
        Map<String, Object> idValues = filterNodeIdProperties(document, path);

        // retrieve the current node
        final Node node;
        if (this.config.isWrite()) {
            node = documentToNodes.getOrCreateRealNode(labels, idValues);
        } else {
            node = documentToNodes.getOrCreateVirtualNode(nodes, labels, idValues);
        }

        // write node properties
        document.entrySet().stream()
                .filter(e -> isSimpleType(e, path))
                .flatMap(e -> {
                    if (e.getValue() instanceof Map) {
                        return Util.flattenMap((Map<String, Object>) e.getValue(), e.getKey()).entrySet().stream();
                    } else {
                        return Stream.of(e);
                    }
                })
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
                .filter(e -> !isSimpleType(e, path))
                .forEach(e -> {
                    String newPath = path + "."  + e.getKey();
                    if (e.getValue() instanceof Map) { // if value is a complex object (map)
                        Map inner = (Map) e.getValue();
                        fromDocument(inner, node, e.getKey(), nodes, relationships, newPath);
                    } else {
                        List<Map> list = (List) e.getValue(); // if value is and array
                        list.forEach(map -> fromDocument(map, node, e.getKey(), nodes, relationships, newPath));
                    }
                });

        Set<Node> nodesWithSameIds = getNodesWithSameLabels(nodes, labels);
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

    public void prepareData(Map<String, Object> document, String path) {
        if (config.isGenerateId()) {
            List<String> ids = config.idsForPath(path);
            String idField;
            if (ids.isEmpty()) {
                idField = config.getIdField();
            } else {
                idField = ids.get(0);
            }
            document.computeIfAbsent(idField, key -> UUID.randomUUID().toString());
        }
    }

    private Map<String, Object> filterNodeIdProperties(Map<String, Object> document, String path) {
        List<String> ids = config.idsForPath(path);
        Map<String, Object> idMap = new HashMap<>(document);
        if(ids.isEmpty()) {
            idMap.keySet().retainAll(Collections.singleton(config.getIdField()));
        } else {
            idMap.keySet().retainAll(ids);
        }
        return idMap;
    }

    public static Set<Node> getNodesWithSameLabels(Map<Set<String>, Set<Node>> nodes, Label[] labels) {
        Set<String> set = Stream.of(labels).map(Label::name).collect(Collectors.toSet());
        return nodes.computeIfAbsent(set, (k) -> new LinkedHashSet<>());
    }


    private boolean isSimpleType(Map.Entry<String, Object> e, String path) {
        List<String> valueObjects = config.valueObjectForPath(path);
        if (e.getValue() instanceof Map) {
            return valueObjects.contains(e.getKey());
        }
        if (e.getValue() instanceof List) {
            List list = (List) e.getValue();
            if (!list.isEmpty()) {
                Object object = list.get(0); // assumption: homogeneous array
                if (object instanceof Map) { // if is an array of complex type
                    return false; // TODO add support for array of value objects
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

    public VirtualGraph createWithoutMutatingOriginal(Object documentObj) {
        List<Map<String, Object>> original = getDocumentCollection(documentObj);

        List<Map<String, Object>> coll = original.stream().map(HashMap::new).collect(Collectors.toList());

        return getVirtualGraph(coll);
    }

    public VirtualGraph create(Object documentObj) {
        List<Map<String, Object>> coll = getDocumentCollection(documentObj);
        return getVirtualGraph(coll);
    }

    private VirtualGraph getVirtualGraph(List<Map<String, Object>> coll) {
        Map<Set<String>, Set<Node>> nodes = new LinkedHashMap<>();
        Set<Relationship> relationships = new LinkedHashSet<>();
        coll.forEach(map -> fromDocument(map, null, null, nodes, relationships, JSON_ROOT));
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

    private Map<String, List<Map<String, Object>>> flatMapFieldsWithPath(Map<String, Object> map, String path) {
        Map<String, List<Map<String, Object>>> flatWithPath = new HashMap<>();
        String newPath = path == null ? JSON_ROOT : path;
        flatWithPath.computeIfAbsent(newPath, e -> new ArrayList<>()).add(map);
        Map<String, List<Map<String, Object>>> collect = map.entrySet()
                .stream()
                .filter(e -> !isSimpleType(e, path))
                .flatMap(e -> {
                    String subPath = newPath + "." + e.getKey();
                    if (e.getValue() instanceof Map) {
                        return flatMapFieldsWithPath((Map<String, Object>) e.getValue(), subPath).entrySet().stream();
                    } else {
                        List<Map<String, Object>> list = (List<Map<String, Object>>) e.getValue();
                        return list.stream().flatMap(le -> flatMapFieldsWithPath(le, subPath).entrySet().stream());
                    }
                })
                .flatMap(e -> e.getValue().stream().map(ee -> new AbstractMap.SimpleEntry<>(e.getKey(), ee)))
                .collect(Collectors.groupingBy(e -> e.getKey(),
                        Collectors.mapping(e -> e.getValue(), Collectors.toList())));
        flatWithPath.putAll(collect);

        return flatWithPath;
    }

    public Map<Long, String> validate(Object doc) {
        AtomicLong index = new AtomicLong(-1);
        return getDocumentCollection(doc).stream()
                .map(elem -> {
                    long line = index.incrementAndGet();
                    prepareData(elem, JSON_ROOT);
                    // id, label validation
                    Map<Map<String, Object>, List<String>> errors = validate(elem, JSON_ROOT);
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

    public static class DocumentToNodes {
        private final Map<Set<String>, Set<Node>> initialNodes;
        private final Transaction tx;

        public DocumentToNodes(Set<Node> initialNodes, Transaction tx) {
            this.tx = tx;
            this.initialNodes = new HashMap<>();
            for (Node initialNode : initialNodes) {
                Set<String> labels = StreamSupport.stream(initialNode.getLabels().spliterator(), false).map(Label::name).collect(Collectors.toSet());
                if(this.initialNodes.containsKey(labels)) {
                    this.initialNodes.get(labels).add(initialNode);
                } else {
                    this.initialNodes.put(labels, new HashSet<>(Arrays.asList(initialNode)));
                }
            }
        }

        public Node getOrCreateRealNode(Label[] labels, Map<String, Object> idValues) {
            return Stream.of(labels)
                    .map(label -> tx.findNodes(label, idValues))
                    .filter(it -> it.hasNext())
                    .map(it -> it.next())
                    .findFirst()
                    .orElseGet(() -> tx.createNode(labels));
        }

        public Node getOrCreateVirtualNode(Map<Set<String>, Set<Node>> nodes, Label[] labels, Map<String, Object> idValues) {
            Set<Node> nodesWithSameIds = getNodesWithSameLabels(nodes, labels);
            Set<Node> initialNodesWithSameIds = getNodesWithSameLabels(this.initialNodes, labels);

            HashSet<Node> searchableNodes = new HashSet<>(nodesWithSameIds);
            searchableNodes.addAll(initialNodesWithSameIds);

            return searchableNodes
                    .stream()
                    .filter(n -> {
                        if (Stream.of(labels).anyMatch(label -> n.hasLabel(label))) {
                            Map<String, Object> ids = filterNodeIdProperties(n, idValues);
                            return idValues.equals(ids);
                        }
                        return StreamSupport.stream(n.getRelationships().spliterator(), false)
                                .anyMatch(r -> {
                                    Node otherNode = r.getOtherNode(n);
                                    Map<String, Object> ids = filterNodeIdProperties(otherNode, idValues);
                                    return Stream.of(labels).anyMatch(label -> otherNode.hasLabel(label)) && idValues.equals(ids);
                                });
                    })
                    .findFirst()
                    .orElseGet(() -> new VirtualNode(labels, Collections.emptyMap()));
        }

        private Map<String, Object> filterNodeIdProperties(Node n, Map<String, Object> idMap) {
            return n.getProperties(idMap.keySet().toArray(new String[idMap.keySet().size()]));
        }

        private Set<Node> getNodesWithSameLabels(Map<Set<String>, Set<Node>> nodes, Label[] labels) {
            Set<String> set = Stream.of(labels).map(Label::name).collect(Collectors.toSet());
            return nodes.computeIfAbsent(set, (k) -> new LinkedHashSet<>());
        }

    }

}
