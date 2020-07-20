package apoc.hashing;

import apoc.util.Util;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Fingerprinting {

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @UserFunction
    @Description("calculate a checksum (md5) over a node or a relationship. This deals gracefully with array properties. Two identical entities do share the same hash.")
    public String fingerprint(@Name("some object") Object thing, @Name(value = "propertyExcludes", defaultValue = "[]") List<String> excludedPropertyKeys) {
        FingerprintingConfig config = new FingerprintingConfig(Util.map("allNodesDisallowList", excludedPropertyKeys,
                "allRelsDisallowList", excludedPropertyKeys, "mapDisallowList", excludedPropertyKeys,
                "strategy", FingerprintingConfig.FingerprintStrategy.EAGER.toString()));
        return fingerprint(thing, config);
    }

    @UserFunction
    @Description("calculate a checksum (md5) over a node or a relationship. This deals gracefully with array properties. Two identical entities do share the same hash.")
    public String fingerprinting(@Name("some object") Object thing, @Name(value = "conf", defaultValue = "{}") Map<String, Object> conf) {
        FingerprintingConfig config = new FingerprintingConfig(conf);
        return fingerprint(thing, config);
    }

    private String fingerprint(Object thing, FingerprintingConfig config) {
        return withMessageDigest(config, md -> fingerprint(md, thing, config));
    }

    private void fingerprint(DiagnosingMessageDigestDecorator md, Object thing, FingerprintingConfig conf) {
        if (thing instanceof Node) {
            fingerprintNode(md, (Node) thing, conf);
        } else if (thing instanceof Relationship) {
            fingerprintRelationship(md, (Relationship) thing, conf);
        } else if (thing instanceof Path) {
            fingerprintPath(md, (Path) thing, conf);
        } else if (thing instanceof Map) {
            fingerprintMap(md, conf, (Map<String, Object>) thing);
        } else if (thing instanceof List) {
            fingerprintList(md, conf, (List) thing);
        } else {
            md.update(convertValueToString(thing).getBytes());
        }
    }

    private void fingerprintList(DiagnosingMessageDigestDecorator md, FingerprintingConfig conf, List list) {
        list.stream().forEach(o -> fingerprint(md, o, conf));
    }

    private void fingerprintPath(DiagnosingMessageDigestDecorator md, Path thing, FingerprintingConfig conf) {
        StreamSupport.stream(thing.nodes().spliterator(), false)
                .forEach(o -> fingerprint(md, o, conf));
        StreamSupport.stream(thing.relationships().spliterator(), false)
                .forEach(o -> fingerprint(md, o, conf));
    }

    private void fingerprintMap(DiagnosingMessageDigestDecorator md, FingerprintingConfig conf, Map<String, Object> map) {
        map.entrySet().stream()
                .filter(e -> {
                    if (!conf.getMapAllowList().isEmpty()) {
                        return conf.getMapAllowList().contains(e.getKey());
                    } else {
                        return !conf.getMapDisallowList().contains(e.getKey());
                    }
                })
                .sorted(Map.Entry.comparingByKey())
                .forEachOrdered(entry -> {
                    md.update(entry.getKey().getBytes());
                    md.update(fingerprint(entry.getValue(), conf).getBytes());
                });
    }

    @UserFunction
    @Description("calculate a checksum (md5) over a the full graph. Be aware that this function does use in-memomry datastructures depending on the size of your graph.")
    public String fingerprintGraph(@Name(value = "propertyExcludes", defaultValue = "[]") List<String> excludedPropertyKeys) {
        FingerprintingConfig config = new FingerprintingConfig(Util.map("allNodesDisallowList", excludedPropertyKeys,
                "allRelsDisallowList", excludedPropertyKeys, "mapDisallowList", excludedPropertyKeys,
                "strategy", FingerprintingConfig.FingerprintStrategy.EAGER.toString()));
        return withMessageDigest(config, messageDigest -> {
            // step 1: load all nodes, calc their hash and map them to id
            Map<Long, String> idToNodeHash = tx.getAllNodes().stream().collect(Collectors.toMap(
                    Node::getId,
                    node -> fingerprint(node, config),
                    (aLong, aLong2) -> {
                        throw new RuntimeException();
                    },
                    () -> new TreeMap<>()
            ));

            // step 2: build inverse map
            Map<String, Long> nodeHashToId = idToNodeHash.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey,
                    (o, o2) -> {
                        throw new RuntimeException();
                    },
                    () -> new TreeMap<>()
            ));

            // step 3: iterate nodes in order of their hash (we cannot rely on internal ids)
            nodeHashToId.entrySet().stream().forEach(entry -> {
                messageDigest.update(entry.getKey().getBytes());

                Node node = tx.getNodeById(entry.getValue());
                List<EndNodeRelationshipHashTuple> endNodeRelationshipHashTuples = StreamSupport.stream(node.getRelationships(Direction.OUTGOING).spliterator(), false)
                        .map(relationship -> {
                            String endNodeHash = idToNodeHash.get(relationship.getEndNodeId());
                            String relationshipHash = fingerprint(relationship, excludedPropertyKeys);
                            return new EndNodeRelationshipHashTuple(endNodeHash, relationshipHash);
                        }).collect(Collectors.toList());

                endNodeRelationshipHashTuples.stream().sorted().forEach(endNodeRelationshipHashTuple -> {
                    messageDigest.update(endNodeRelationshipHashTuple.getEndNodeHash().getBytes());
                    messageDigest.update(endNodeRelationshipHashTuple.getRelationshipHash().getBytes());
                });

            });

        });
    }

    private static class EndNodeRelationshipHashTuple implements Comparable {
        private final String endNodeHash;
        private final String relationshipHash;

        public EndNodeRelationshipHashTuple(String endNodeHash, String relationshipHash) {
            this.endNodeHash = endNodeHash;
            this.relationshipHash = relationshipHash;
        }

        @Override
        public int compareTo(Object o) {
            EndNodeRelationshipHashTuple other = (EndNodeRelationshipHashTuple) o;
            int res = endNodeHash.compareTo(other.endNodeHash);
            if (res == 0) {
                res = relationshipHash.compareTo(other.relationshipHash);
            }
            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EndNodeRelationshipHashTuple that = (EndNodeRelationshipHashTuple) o;

            if (endNodeHash != null ? !endNodeHash.equals(that.endNodeHash) : that.endNodeHash != null) return false;
            return relationshipHash != null ? relationshipHash.equals(that.relationshipHash) : that.relationshipHash == null;
        }

        @Override
        public int hashCode() {
            int result = endNodeHash != null ? endNodeHash.hashCode() : 0;
            result = 31 * result + (relationshipHash != null ? relationshipHash.hashCode() : 0);
            return result;
        }

        public String getEndNodeHash() {
            return endNodeHash;
        }

        public String getRelationshipHash() {
            return relationshipHash;
        }
    }

    private void fingerprintNode(DiagnosingMessageDigestDecorator md, Node node, FingerprintingConfig config) {
        switch (config.getStrategy()) {
            case EAGER:
                StreamSupport.stream(node.getLabels().spliterator(), false)
                        .map(Label::name)
                        .sorted()
                        .map(String::getBytes)
                        .forEach(md::update);
                break;
            case LAZY:
                StreamSupport.stream(node.getLabels().spliterator(), false)
                        .map(Label::name)
                        .filter(name -> config.getAllLabels().contains(name))
                        .sorted()
                        .map(String::getBytes)
                        .forEach(md::update);
        }

        final List<String> keysToRetain = new ArrayList<>(config.getAllNodesAllowList());
        keysToRetain.addAll(StreamSupport.stream(node.getLabels().spliterator(), false)
                .map(Label::name)
                .flatMap(label -> config.getNodeAllowMap().getOrDefault(label, Collections.emptyList()).stream())
                .collect(Collectors.toSet()));

        final List<String> keysToRemove = new ArrayList<>(config.getAllNodesDisallowList());
        keysToRemove.addAll(StreamSupport.stream(node.getLabels().spliterator(), false)
                .map(Label::name)
                .flatMap(label -> config.getNodeDisallowMap().getOrDefault(label, Collections.emptyList()).stream())
                .collect(Collectors.toSet()));
        keysToRemove.addAll(config.getMapDisallowList()); // just to backwards compatibility remove it

        final Map<String, Object> allProperties = getEntityProperties(node, config, keysToRetain, keysToRemove);
        fingerprint(md, allProperties, config);
    }

    private void fingerprintRelationship(DiagnosingMessageDigestDecorator md, Relationship rel, FingerprintingConfig config) {
        switch (config.getStrategy()) {
            case EAGER:
                md.update(rel.getType().name().getBytes());
                md.update(fingerprint(rel.getStartNode(), config).getBytes());
                md.update(fingerprint(rel.getEndNode(), config).getBytes());
                break;
            case LAZY:
                if (config.getAllTypes().contains(rel.getType().name())) {
                    md.update(rel.getType().name().getBytes());
                    md.update(fingerprint(rel.getStartNode(), config).getBytes());
                    md.update(fingerprint(rel.getEndNode(), config).getBytes());
                }
        }

        final List<String> keysToRetain = new ArrayList<>(config.getAllRelsAllowList());
        keysToRetain.addAll(config.getRelAllowMap()
                .getOrDefault(rel.getType().name(), Collections.emptyList()));

        final List<String> keysToRemove = new ArrayList<>(config.getAllRelsDisallowList());
        keysToRemove.addAll(config.getRelDisallowMap()
                .getOrDefault(rel.getType().name(), Collections.emptyList()));
        keysToRemove.addAll(config.getMapDisallowList()); // just to backwards compatibility remove it

        final Map<String, Object> allProperties = getEntityProperties(rel, config, keysToRetain, keysToRemove);
        fingerprint(md, allProperties, config);
    }

    private Map<String, Object> getEntityProperties(Entity entity, FingerprintingConfig config, List<String> keysToRetain, List<String> keysToRemove) {
        final Map<String, Object> allProperties;
        if (keysToRetain.isEmpty() && keysToRemove.isEmpty()) {
            switch (config.getStrategy()) {
                case LAZY:
                    allProperties = Collections.emptyMap();
                    break;
                default:
                    allProperties = entity.getAllProperties();
            }
        } else {
            allProperties = entity.getAllProperties();
            if (!keysToRetain.isEmpty()) {
                allProperties.keySet().retainAll(keysToRetain);
            }
            if (!keysToRemove.isEmpty()) {
                allProperties.keySet().removeAll(keysToRemove);
            }
        }
        return allProperties;
    }

    private String withMessageDigest(FingerprintingConfig conf, Consumer<DiagnosingMessageDigestDecorator> consumer) {
        try {
            MessageDigest md = MessageDigest.getInstance(conf.getDigestAlgorithm());
            DiagnosingMessageDigestDecorator dmd = new DiagnosingMessageDigestDecorator(md);
            consumer.accept(dmd);
            return renderAsHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String renderAsHex(byte[] content) {
        Formatter formatter = new Formatter();
        for (byte b : content) {
            formatter.format("%02X", b);
        }
        return formatter.toString();
    }

    private String convertValueToString(Object value) {
        if (value == null) {
            return "";
        }
        if (value.getClass().isArray()) {
            return nativeArrayToString(value);
        } else {
            return value.toString();
        }
    }

    private String nativeArrayToString(Object value) {
        StringBuilder sb = new StringBuilder();
        if (value instanceof String[]) {
            for (String s : (String[]) value) {
                sb.append(s);
            }
        } else if (value instanceof double[]) {
            for (double d : (double[]) value) {
                sb.append(d);
            }
        } else if (value instanceof long[]) {
            for (double l : (long[]) value) {
                sb.append(l);
            }
        } else {
            throw new UnsupportedOperationException("cannot yet deal with " + value.getClass().getName());
        }
        return sb.toString();
    }

    /**
     * if debug log level is enabled, send all updates to the message digest to the log as well for diagnosis
     */
    private class DiagnosingMessageDigestDecorator {
        private final MessageDigest delegate;

        public DiagnosingMessageDigestDecorator(MessageDigest delegate) {
            this.delegate = delegate;
        }

        public void update(byte[] value) {
            if (log.isDebugEnabled()) {
                log.debug("adding to message digest {}", new String(value));
            }
            delegate.update(value);
        }
    }
}