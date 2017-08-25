package apoc.nodes;

import apoc.Description;
import apoc.Pools;
import apoc.result.GraphResult;
import apoc.result.VirtualNode;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

/**
 * @author mh
 * @since 14.06.17
 */
public class Grouping {

    private static final int BATCHSIZE = 10000;
    @Context
    public GraphDatabaseAPI db;
    @Context
    public Log log;

    static class Key {
        private final int hash;
        private final String label;
        private final Map<String,Object> values;

        public Key(String label, Map<String, Object> values) {
            this.label = label;
            this.values = values;
            hash = 31 * label.hashCode() + values.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;
            return label.equals(key.label) && values.equals(key.values);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    @Procedure
    @Description("Group all nodes and their relationships by given keys, create virtual nodes and relationships for the summary information, you can provide an aggregations map [{kids:'sum',age:['min','max','avg'],gender:'collect'},{`*`,'count'}]")
    public Stream<GraphResult> group(@Name("labels") List<String> labels, @Name("groupByProperties") List<String> groupByProperties,
                                     @Name(value = "aggregations",defaultValue = "[{\"*\":\"count\"},{\"*\":\"count\"}]") List<Map<String,Object>> aggregations) {
        String[] keys = groupByProperties.toArray(new String[groupByProperties.size()]);
        Map<String,List<String>> nodeAggNames = (aggregations.size()>0) ? toStringListMap(aggregations.get(0)) : emptyMap();
        String[] nodeAggKeys = keyArray(nodeAggNames, "*");

        Map<String,List<String>> relAggNames = (aggregations.size()>1) ? toStringListMap(aggregations.get(1)) : emptyMap();
        String[] relAggKeys = keyArray(relAggNames, "*");;

        Map<Key,Set<Node>> grouped = new ConcurrentHashMap<>();
        Map<Key,Node> virtual = new ConcurrentHashMap<>();
        Map<RelKey,Relationship> virtualRels = new ConcurrentHashMap<>();

        List<Future> futures = new ArrayList<>(1000);

        ExecutorService pool = Pools.DEFAULT;
        for (String labelName : labels) {
            Label label = Label.label(labelName);
            Label[] singleLabel = {label};

            try (ResourceIterator<Node> nodes = db.findNodes(label)) {
                while (nodes.hasNext()) {
                    List<Node> batch = Util.take(nodes, BATCHSIZE);
                    futures.add(Util.inTxFuture(pool, db, () -> {
                        try {
                            for (Node node : batch) {
                                Key key = keyFor(node, labelName, keys);
                                grouped.compute(key, (k, v) -> {if (v == null) v = new HashSet<>(); v.add(node); return v;});
                                virtual.compute(key, (k, v) -> {
                                    if (v == null) {
                                        v = new VirtualNode(singleLabel,node.getProperties(keys),db);
                                    }
                                    Node vn = v;
                                    if (!nodeAggNames.isEmpty()) {
                                        aggregate(vn, nodeAggNames, nodeAggKeys.length > 0 ? node.getProperties(nodeAggKeys) : Collections.emptyMap());
                                    }
                                    return vn;}
                                );
                            }
                        } catch(Exception e) {
                            log.debug("Error grouping nodes",e);
                        }
                        return null;
                    }));
                    Util.removeFinished(futures);
                }
            }
        }
        Util.waitForFutures(futures);
        futures.clear();
        Iterator<Map.Entry<Key, Set<Node>>> entries = grouped.entrySet().iterator();
        int size = 0;
        List<Map.Entry<Key, Set<Node>>> batch = new ArrayList<>();
        while (entries.hasNext()) {
            Map.Entry<Key, Set<Node>> outerEntry = entries.next();
            batch.add(outerEntry);
            size += outerEntry.getValue().size();
            if (size > BATCHSIZE || !entries.hasNext()) {
                ArrayList<Map.Entry<Key, Set<Node>>> submitted = new ArrayList<>(batch);
                batch.clear();
                size = 0;
                futures.add(Util.inTxFuture(pool, db, () -> {
                    try {
                        for (Map.Entry<Key, Set<Node>> entry : submitted) {
                            for (Node node : entry.getValue()) {
                                Key startKey = entry.getKey();
                                Node v1 = virtual.get(startKey);
                                for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                                    Node endNode = rel.getEndNode();
                                    for (Key endKey : keysFor(endNode, labels, keys)) {
                                        Node v2 = virtual.get(endKey);
                                        if (v2 == null) continue;
                                        virtualRels.compute(new RelKey(startKey, endKey, rel), (rk, vRel) -> {
                                            if (vRel == null) vRel = v1.createRelationshipTo(v2, rel.getType());
                                            if (!relAggNames.isEmpty()) {
                                                aggregate(vRel, relAggNames, relAggKeys.length > 0 ? rel.getProperties(relAggKeys) : Collections.emptyMap());
                                            }
                                            return vRel;
                                        });
                                    }
                                }
                            }
                        }
                    } catch(Exception e) {
                        log.debug("Error grouping relationships",e);
                    }
                    return null;
                }));
                Util.removeFinished(futures);
            }
        }
        Util.waitForFutures(futures);
        return fixAggregates(virtual.values()).stream().map( n -> new GraphResult(singletonList(n), fixAggregates(Iterables.asList(n.getRelationships()))));
    }

    public Map<String, List<String>> toStringListMap(Map<String, Object> input) {
        Map<String, List<String>> nodeAggNames = new LinkedHashMap<>(input.size());
        input.forEach((k, v) -> nodeAggNames.put(k, v instanceof List ? ((List<Object>) v).stream().map(Object::toString).collect(Collectors.toList()) : singletonList(v.toString())));
        return nodeAggNames;
    }

    public String[] keyArray(Map<String, ?> map, String... removeKeys) {
        List<String> keys = new ArrayList<>(map.keySet());
        for (String key : removeKeys) keys.remove(key);
        return keys.toArray(new String[keys.size()]);
    }

    private <C extends Collection<T>,T extends PropertyContainer> C fixAggregates(C pcs) {
        for (PropertyContainer pc : pcs) {
            pc.getAllProperties().entrySet().forEach((entry) -> {
                Object v = entry.getValue();
                String k = entry.getKey();
                if (k.matches("^(min|max|sum)_.+")) {
                    if (v instanceof Number && ((Number) v).doubleValue() == ((Number) v).longValue())
                        entry.setValue(((Number) v).longValue());
                }
                if (k.matches("^avg_.+") && v instanceof double[]) {
                    double[] values = (double[]) v;
                    entry.setValue(values[1] == 0 ? 0 : values[0]/ values[1]);
                }
                if (k.matches("^collect_.+") && v instanceof Collection) {
                    entry.setValue(((Collection)v).toArray());
                }
            });
        }
        return pcs;
    }

    public void aggregate(PropertyContainer pc, Map<String, List<String>> aggregations, Map<String, Object> properties) {
        aggregations.forEach((k2,aggNames) -> {
            for (String aggName : aggNames) {
                String key = aggName + "_" + k2;
                if ("count_*".equals(key)) {
                    pc.setProperty(key, ((Number) pc.getProperty(key, 0)).longValue() + 1);
                } else {
                    Object value = properties.get(k2);
                    if (value != null) {
                        switch (aggName) {
                            case "collect":
                                List<Object> existing = (List<Object>) pc.getProperty(key, new ArrayList<>());
                                existing.add(value);
                                pc.setProperty(key, existing);
                                break;
                            case "count":
                                pc.setProperty(key, ((Number) pc.getProperty(key, 0)).longValue() + 1);
                                break;
                            case "sum":
                                pc.setProperty(key, ((Number) pc.getProperty(key, 0)).doubleValue() + Util.toDouble(value));
                                break;
                            case "min":
                                pc.setProperty(key, Math.min(((Number) pc.getProperty(key, Double.MAX_VALUE)).doubleValue(), Util.toDouble(value)));
                                break;
                            case "max":
                                pc.setProperty(key, Math.max(((Number) pc.getProperty(key, Double.MIN_VALUE)).doubleValue(), Util.toDouble(value)));
                                break;
                            case "avg": {
                                double[] avg = (double[]) pc.getProperty(key, new double[2]);
                                avg[0] += Util.toDouble(value);
                                avg[1] += 1;
                                pc.setProperty(key, avg);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    public Key keyFor(Node node, String label, String[] keys) {
        Map<String, Object> props = node.getProperties(keys);
        return new Key(label, props);
    }

    public Collection<Key> keysFor(Node node, List<String> labels, String[] keys) {
        Map<String, Object> props = node.getProperties(keys);
        List<Key> result=new ArrayList<>(labels.size());
        for (Label label : node.getLabels()) {
            if (labels.contains(label.name())) {
                result.add(new Key(label.name(), props));
            }
        }
        return result;
    }

    private static class RelKey {
        private final int hash;
        private final Key startKey;
        private final Key endKey;
        private final String type;

        public RelKey(Key startKey, Key endKey, Relationship rel) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.type = rel.getType().name();
            hash = 31 * (31 * startKey.hashCode() + endKey.hashCode()) + type.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RelKey relKey = (RelKey) o;

            return startKey.equals(relKey.startKey) && endKey.equals(relKey.endKey) && type.equals(relKey.type);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
