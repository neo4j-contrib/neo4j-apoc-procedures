package apoc.hashing;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Fingerprinting {

    public static final String DIGEST_ALGORITHM = "MD5";

    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("calculate a checksum (md5) over a node or a relationship. This deals gracefully with array properties. Two identical entities do share the same hash.")
    public String fingerprintNodeOrRel(@Name("node_relation") Object thing, @Name(value = "propertyExcludes", defaultValue = "") List<String> excludedPropertyKeys) {

        if (thing instanceof Node) {
            return fingerprintNode((Node) thing, excludedPropertyKeys);
        } else if (thing instanceof Relationship) {
                return fingerprintRelationship((Relationship) thing, excludedPropertyKeys);
        } else {
            throw new UnsupportedOperationException("cannot fingerprint a " + thing.getClass());
        }

    }

    @UserFunction
    @Description("calculate a checksum (md5) over a the full graph. Be aware that this function does use in-memomry datastructures depending on the size of your graph.")
    public String fingerprintGraph(@Name(value = "propertyExcludes", defaultValue = "") List<String> excludedPropertyKeys) {

        return withMessageDigest(messageDigest -> {
            // step 1: load all nodes, calc their hash and map them to id
            Map<Long, String> idToNodeHash = db.getAllNodes().stream().collect(Collectors.toMap(
                    Node::getId,
                    node -> fingerprintNode(node, excludedPropertyKeys),
                    (aLong, aLong2) -> { throw new RuntimeException(); },
                    () -> new TreeMap<>()
            ));

            // step 2: build inverse map
            Map<String, Long> nodeHashToId = idToNodeHash.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getValue,
                    Map.Entry::getKey,
                    (o, o2) -> { throw new RuntimeException(); },
                    () -> new TreeMap<>()
            ));

            // step 3: iterate nodes in order of their hash (we cannot rely on internal ids)
            nodeHashToId.entrySet().stream().forEach(entry ->{
                messageDigest.update(entry.getKey().getBytes());

                Node node = db.getNodeById(entry.getValue());
                List<EndNodeRelationshipHashTuple> endNodeRelationshipHashTuples = StreamSupport.stream(node.getRelationships(Direction.OUTGOING).spliterator(), false)
                        .map(relationship -> {
                            String endNodeHash = idToNodeHash.get(relationship.getEndNodeId());
                            String relationshipHash = fingerprintRelationship(relationship, excludedPropertyKeys);
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

    private String fingerprintNode(Node node, List<String> excludedPropertyKeys) {
        String s = withMessageDigest(md -> {
            StreamSupport.stream(node.getLabels().spliterator(), false)
                    .map(Label::name)
                    .sorted()
                    .map(String::getBytes)
                    .forEach(md::update);
            addPropertiesToDigest(node, excludedPropertyKeys, md);
        });
//        System.out.println("md " + s + " for node " + node.getLabels() + " " + node.getAllProperties());
        return s;
    }

    private String fingerprintRelationship(Relationship rel, List<String> excludedPropertyKeys) {
        String s = withMessageDigest(md -> {
            md.update(rel.getType().name().getBytes());
            addPropertiesToDigest(rel, excludedPropertyKeys, md);
        });
//        System.out.println("md " + s + " for rel " + rel.getType() + " " + rel.getAllProperties());
        return s;

    }

    private void addPropertiesToDigest(PropertyContainer propertyContainer, List<String> excludedPropertyKeys, MessageDigest md) {
        propertyContainer.getAllProperties().entrySet().stream()
                .filter(e -> !excludedPropertyKeys.contains(e.getKey()))
                .sorted(Map.Entry.comparingByKey())
                .forEachOrdered(entry -> {
                    md.update(entry.getKey().getBytes());
                    md.update(convertValueToString(entry.getValue()).getBytes());
                });
    }

    private String withMessageDigest(Consumer<MessageDigest> consumer) {
        try {
            MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
            consumer.accept(md);
            return DatatypeConverter.printHexBinary(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertValueToString(Object value) {
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

}
