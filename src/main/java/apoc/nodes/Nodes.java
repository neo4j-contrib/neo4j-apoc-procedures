package apoc.nodes;

import apoc.create.Create;
import apoc.path.RelationshipTypeAndDirections;
import apoc.refactor.util.PropertiesManager;
import apoc.refactor.util.RefactorConfig;
import apoc.result.*;
import apoc.util.Util;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.*;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.path.RelationshipTypeAndDirections.format;
import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.refactor.util.RefactorUtil.copyProperties;
import static apoc.util.Util.map;
import static java.util.stream.Collectors.*;

public class Nodes {

    @Context public GraphDatabaseService db;
    @Context public KernelTransaction ktx;

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.nodes.link([nodes],'REL_TYPE') - creates a linked list of nodes from first to last")
    public void link(@Name("nodes") List<Node> nodes, @Name("type") String type) {
        Iterator<Node> it = nodes.iterator();
        if (it.hasNext()) {
            RelationshipType relType = RelationshipType.withName(type);
            Node node = it.next();
            while (it.hasNext()) {
                Node next = it.next();
                node.createRelationshipTo(next, relType);
                node = next;
            }
        }
    }

    @Procedure
    @Description("apoc.nodes.get(node|nodes|id|[ids]) - quickly returns all nodes with these ids")
    public Stream<NodeResult> get(@Name("nodes") Object ids) {
        return Util.nodeStream(db, ids).map(NodeResult::new);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.nodes.delete(node|nodes|id|[ids]) - quickly delete all nodes with these ids")
    public Stream<LongResult> delete(@Name("nodes") Object ids, @Name("batchSize") long batchSize) {
        Iterator<Node> it = Util.nodeStream(db, ids).iterator();
        long count = 0;
        while (it.hasNext()) {
            final List<Node> batch = Util.take(it, (int)batchSize);
//            count += Util.inTx(api,() -> batch.stream().peek( n -> {n.getRelationships().forEach(Relationship::delete);n.delete();}).count());
            count += Util.inTx(db,() -> {db.execute("FOREACH (n in {nodes} | DETACH DELETE n)",map("nodes",batch)).close();return batch.size();});
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure
    @Description("apoc.get.rels(rel|id|[ids]) - quickly returns all relationships with these ids")
    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(db, ids).map(RelationshipResult::new);
    }

    @UserFunction("apoc.node.relationship.exists")
    @Description("apoc.node.relationship.exists(node, rel-direction-pattern) - returns true when the node has the relationships of the pattern")
    public boolean hasRelationship(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types) {
        if (types == null || types.isEmpty()) return node.hasRelationship();
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            if (node.hasRelationship(pair.first(), pair.other())) return true;
        }
        return false;
    }

    @UserFunction("apoc.node.relationships.exist")
    @Description("apoc.node.relationships.exist(node, rel-direction-pattern) - returns a map of relationship-pattenr -> true/false for each pair")
    public Map<String,Boolean> hasRelationships(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types) {
        if (types == null || types.isEmpty()) return Collections.emptyMap();
        Map<String,Boolean> result = new HashMap<>();
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            result.put(format(pair),node.hasRelationship(pair.first(), pair.other()));
        }
        return result;
    }

    @UserFunction("apoc.nodes.connected")
    @Description("apoc.nodes.connected(start, end, rel-direction-pattern) - returns true when the node is connected to the other node, optimized for dense nodes")
    public boolean connected(@Name("start") Node start, @Name("start") Node end, @Name(value = "types", defaultValue = "") String types)  {
        if (start == null || end == null) return false;
        if (start.equals(end)) return true;

        long startId = start.getId();
        long endId = end.getId();
        List<Pair<RelationshipType, Direction>> pairs = (types == null || types.isEmpty()) ? null : parse(types);

        Read dataRead = ktx.dataRead();
        TokenRead tokenRead = ktx.tokenRead();
        CursorFactory cursors = ktx.cursors();

        try (NodeCursor startNodeCursor = cursors.allocateNodeCursor();
             NodeCursor endNodeCursor = cursors.allocateNodeCursor()) {

            dataRead.singleNode(startId, startNodeCursor);
            if (!startNodeCursor.next()) {
                throw new IllegalArgumentException("node with id " + startId + " does not exist.");
            }

            boolean startDense = startNodeCursor.isDense();
            dataRead.singleNode(endId, endNodeCursor);
            if (!endNodeCursor.next()) {
                throw new IllegalArgumentException("node with id " + endId + " does not exist.");
            }
            boolean endDense = endNodeCursor.isDense();

            if (!startDense) return connected(startNodeCursor, endId, typedDirections(tokenRead, pairs, true));
            if (!endDense) return connected(endNodeCursor, startId, typedDirections(tokenRead, pairs, false));
            return connectedDense(startNodeCursor, endNodeCursor, typedDirections(tokenRead, pairs, true));
        }
    }

    @Procedure
    @Description("apoc.nodes.collapse([nodes...],[{properties:'overwrite' or 'discard' or 'combine'}]) yield from, rel, to merge nodes onto first in list")
    public Stream<VirtualPathResult> collapse(@Name("nodes") List<Node> nodes, @Name(value = "config", defaultValue = "") Map<String, Object> config) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        if (nodes.size() == 1) return Stream.of(new VirtualPathResult(nodes.get(0), null, null));
        Set<Node> nodeSet = new LinkedHashSet<>(nodes);
        RefactorConfig conf = new RefactorConfig(config);
        VirtualNode first = createVirtualNode(nodeSet, conf);
        if (first.getRelationships().iterator().hasNext()) {
            return StreamSupport.stream(first.getRelationships().spliterator(), false)
                    .map(relationship -> new VirtualPathResult(relationship.getStartNode(), relationship, relationship.getEndNode()));
        } else {
            return Stream.of(new VirtualPathResult(first, null, null));
        }
    }

    private VirtualNode createVirtualNode(Set<Node> nodes, RefactorConfig conf) {
        Create create = new Create();
        Node first = nodes.iterator().next();
        List<String> labels = Util.labelStrings(first);
        if (conf.isCollapsedLabel()) {
            labels.add("Collapsed");
        }
        VirtualNode virtualNode = (VirtualNode) create.vNodeFunction(labels, first.getAllProperties());
        createVirtualRelationships(nodes, virtualNode, first, conf);
        nodes.stream().skip(1).forEach(node -> {
            virtualNode.addLabels(node.getLabels());
            PropertiesManager.mergeProperties(node.getAllProperties(), virtualNode, conf);
            createVirtualRelationships(nodes, virtualNode, node, conf);
        });
        if (conf.isCountMerge()) {
            virtualNode.setProperty("count", nodes.size());
        }
        return virtualNode;
    }

    private void createVirtualRelationships(Set<Node> nodes, VirtualNode virtualNode, Node node, RefactorConfig refactorConfig) {
        node.getRelationships().forEach(relationship -> {
            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();

            if (nodes.contains(startNode) && nodes.contains(endNode)) {
                if (refactorConfig.isSelfRel()) {
                    createOrMergeVirtualRelationship(virtualNode, refactorConfig, relationship, virtualNode,  Direction.OUTGOING);
                }
            } else {
                if (startNode.getId() == node.getId()) {
                    createOrMergeVirtualRelationship(virtualNode, refactorConfig, relationship, endNode,  Direction.OUTGOING);
                } else {
                    createOrMergeVirtualRelationship(virtualNode, refactorConfig, relationship, startNode,  Direction.INCOMING);
                }
            }
        });
    }

    private void createOrMergeVirtualRelationship(VirtualNode virtualNode, RefactorConfig refactorConfig, Relationship source, Node node, Direction direction) {
        Iterable<Relationship> rels = virtualNode.getRelationships(source.getType(), direction);
        Optional<Relationship> first = StreamSupport.stream(rels.spliterator(), false).filter(relationship -> relationship.getOtherNode(virtualNode).equals(node)).findFirst();
        if (refactorConfig.isMergeVirtualRels() && first.isPresent()) {
            mergeRelationship(source, first.get(), refactorConfig);
        } else {
            if (direction==Direction.OUTGOING)
               copyProperties(source, virtualNode.createRelationshipTo(node, source.getType()));
            if (direction==Direction.INCOMING) 
               copyProperties(source, virtualNode.createRelationshipFrom(node, source.getType()));
        }
    }

    private void mergeRelationship(Relationship source, Relationship target, RefactorConfig refactorConfig) {
        if (refactorConfig.isCountMerge()) {
            target.setProperty("count", (Integer) target.getProperty("count", 0) + 1);
        }
        PropertiesManager.mergeProperties(source.getAllProperties(), target, refactorConfig);
    }

    /**
     * TODO: be more efficient, in
     * @param start
     * @param end
     * @param typedDirections
     * @return
     */
    private boolean connected(NodeCursor start, long end, int[][] typedDirections) {
        try (RelationshipTraversalCursor relationship = ktx.cursors().allocateRelationshipTraversalCursor()) {
            start.allRelationships(relationship);
            while (relationship.next()) {
                if (relationship.neighbourNodeReference() ==end) {
                    if (typedDirections==null) {
                        return true;
                    } else {
                        int direction = relationship.targetNodeReference() == end ? 0 : 1 ;
                        int[] types = typedDirections[direction];
                        if (arrayContains(types, relationship.type())) return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean arrayContains(int[] array, int element) {
        for (int i=0; i<array.length; i++) {
            if (array[i]==element) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param ops
     * @param pairs
     * @param outgoing
     * @return a int[][] where the first index is 0 for outgoing, 1 for incoming. second array contains rel type ids
     */
    private int[][] typedDirections(TokenRead ops, List<Pair<RelationshipType, Direction>> pairs, boolean outgoing) {
        if (pairs==null) return null;
        int from=0;int to=0;
        int[][] result = new int[2][pairs.size()];
        int outIdx = Direction.OUTGOING.ordinal();
        int inIdx = Direction.INCOMING.ordinal();
        for (Pair<RelationshipType, Direction> pair : pairs) {
            int type = ops.relationshipType(pair.first().name());
            if (type == -1) continue;
            if (pair.other() != Direction.INCOMING) {
                result[outIdx][from++]= type;
            }
            if (pair.other() != Direction.OUTGOING) {
                result[inIdx][to++]= type;
            }
        }
        result[outIdx] = Arrays.copyOf(result[outIdx], from);
        result[inIdx] = Arrays.copyOf(result[inIdx], to);
        if (!outgoing) {
            int[] tmp = result[outIdx];
            result[outIdx] = result[inIdx];
            result[inIdx] = tmp;
        }
        return result;
    }

    static class Degree implements Comparable<Degree> {
        public final long node;
        private final long group;
        public final int degree;
        public final long other;

        public Degree(long node, long group, int degree, long other) {
            this.node = node;
            this.group = group;
            this.degree = degree;
            this.other = other;
        }

        @Override
        public int compareTo(Degree o) {
            return Integer.compare(degree, o.degree);
        }

        public boolean isConnected(Read read, RelationshipTraversalCursor relationship) {
            read.relationships(node, group, relationship);
            while (relationship.next()) {
                if (relationship.neighbourNodeReference()==other) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean connectedDense(NodeCursor start, NodeCursor end, int[][] typedDirections) {
        List<Degree> degrees = new ArrayList<>(32);

        Read read = ktx.dataRead();

        try (RelationshipGroupCursor relationshipGroup = ktx.cursors().allocateRelationshipGroupCursor()) {
            addDegreesForNode(read, start, end, degrees, relationshipGroup, typedDirections);
            addDegreesForNode(read, end, start, degrees, relationshipGroup, typedDirections);
        }


        Collections.sort(degrees);
        try (RelationshipTraversalCursor relationship = ktx.cursors().allocateRelationshipTraversalCursor()) {
            for (Degree degree : degrees) {
                if (degree.isConnected(ktx.dataRead(), relationship)) return true;
            }
            return false;
        }
    }

    private void addDegreesForNode(Read dataRead, NodeCursor node, NodeCursor other, List<Degree> degrees, RelationshipGroupCursor relationshipGroup, int[][] typedDirections) {
        long nodeId = node.nodeReference();
        long otherId = other.nodeReference();

        dataRead.relationshipGroups(nodeId, node.relationshipGroupReference(), relationshipGroup);
        while (relationshipGroup.next()) {
            int type = relationshipGroup.type();
            if ((typedDirections==null) || (arrayContains(typedDirections[0], type))) {
                addDegreeWithDirection(degrees, relationshipGroup.outgoingReference(), relationshipGroup.outgoingCount(), nodeId, otherId);
            }

            if ((typedDirections==null) || (arrayContains(typedDirections[1], type))) {
                addDegreeWithDirection(degrees, relationshipGroup.incomingReference(), relationshipGroup.incomingCount(), nodeId, otherId);
            }
        }
    }

    private void addDegreeWithDirection(List<Degree> degrees, long relationshipGroup, int degree, long nodeId, long otherId) {
        if (degree > 0 ) {
            degrees.add(new Degree(nodeId, relationshipGroup, degree, otherId));
        }
    }

    @UserFunction("apoc.node.labels")
    @Description("returns labels for (virtual) nodes")
    public List<String> labels(@Name("node") Node node) {
        if (node == null) return null;
        Iterator<Label> labels = node.getLabels().iterator();
        if (!labels.hasNext()) return Collections.emptyList();
        Label first = labels.next();
        if (!labels.hasNext()) return Collections.singletonList(first.name());
        List<String> result = new ArrayList<>();
        result.add(first.name());
        labels.forEachRemaining(l -> result.add(l.name()));
        return result;
    }

    @UserFunction("apoc.node.id")
    @Description("returns id for (virtual) nodes")
    public Long id(@Name("node") Node node) {
        return (node == null) ? null : node.getId();
    }

    @UserFunction("apoc.rel.id")
    @Description("returns id for (virtual) relationships")
    public Long relId(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getId();
    }

    @UserFunction("apoc.rel.type")
    @Description("returns type for (virtual) relationships")
    public String type(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getType().name();
    }

    @UserFunction("apoc.any.properties")
    @Description("returns properties for virtual and real, nodes, rels and maps")
    public Map<String,Object> properties(@Name("thing") Object thing, @Name(value = "keys",defaultValue = "null") List<String> keys) {
        if (thing == null) return null;
        if (thing instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) thing;
            if (keys != null) map.keySet().retainAll(keys);
            return map;
        }
        if (thing instanceof PropertyContainer) {
            if (keys == null) return ((PropertyContainer) thing).getAllProperties();
            return ((PropertyContainer) thing).getProperties(keys.toArray(new String[keys.size()]));
        }
        return null;
    }

    @UserFunction("apoc.any.property")
    @Description("returns property for virtual and real, nodes, rels and maps")
    public Object property(@Name("thing") Object thing, @Name(value = "key") String key) {
        if (thing == null || key == null) return null;
        if (thing instanceof Map) {
            return ((Map<String, Object>) thing).get(key);
        }
        if (thing instanceof PropertyContainer) {
            return ((PropertyContainer) thing).getProperty(key,null);
        }
        return null;
    }

    @UserFunction("apoc.node.degree")
    @Description("apoc.node.degree(node, rel-direction-pattern) - returns total degrees of the given relationships in the pattern, can use '>' or '<' for all outgoing or incoming relationships")
    public long degree(@Name("node") Node node, @Name(value = "types",defaultValue = "") String types) {
        if (types==null || types.isEmpty()) return node.getDegree();
        long degree = 0;
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            degree += getDegreeSafe(node, pair.first(), pair.other());
        }
        return degree;
    }

    @UserFunction("apoc.node.degree.in")
    @Description("apoc.node.degree.in(node, relationshipName) - returns total number number of incoming relationships")
    public long degreeIn(@Name("node") Node node, @Name(value = "types",defaultValue = "") String type) {

        if (type==null || type.isEmpty()) {
            return node.getDegree(Direction.INCOMING);
        }

        return node.getDegree(RelationshipType.withName(type), Direction.INCOMING);

    }

    @UserFunction("apoc.node.degree.out")
    @Description("apoc.node.degree.out(node, relationshipName) - returns total number number of outgoing relationships")
    public long degreeOut(@Name("node") Node node, @Name(value = "types",defaultValue = "") String type) {

        if (type==null || type.isEmpty()) {
            return node.getDegree(Direction.OUTGOING);
        }

        return node.getDegree(RelationshipType.withName(type), Direction.OUTGOING);

    }


    @UserFunction("apoc.node.relationship.types")
    @Description("apoc.node.relationship.types(node, rel-direction-pattern) - returns a list of distinct relationship types")
    public List<String> relationshipTypes(@Name("node") Node node, @Name(value = "types",defaultValue = "") String types) {
        if (node==null) return null;
        List<String> relTypes = Iterables.asList(Iterables.map(RelationshipType::name, node.getRelationshipTypes()));
        if (types == null || types.isEmpty()) return relTypes;
        List<String> result = new ArrayList<>(relTypes.size());
        for (Pair<RelationshipType, Direction> p : parse(types)) {
            String name = p.first().name();
            if (relTypes.contains(name) && node.hasRelationship(p.first(),p.other())) {
                result.add(name);
            }
        }
        return result;
    }

    @UserFunction("apoc.nodes.relationship.types")
    @Description("apoc.nodes.relationship.types(node, rel-direction-pattern) - returns a list of distinct relationship types")
    public List<List<String>> nodesRelationshipTypes(@Name("nodes") List<Node> nodes, @Name(value = "types",defaultValue = "") String types) {
        if (nodes == null) return null;
        if (nodes.isEmpty()) return Collections.emptyList();
        List<Pair<RelationshipType, Direction>> parsedTypes = parse(types);
        List<List<String>> allResults = new ArrayList<>(nodes.size());

        for (Node node : nodes) {
            List<String> relTypes = Iterables.asList(Iterables.map(RelationshipType::name, node.getRelationshipTypes()));
            if (types == null || types.isEmpty()) allResults.add(relTypes);
            else {
                List<String> result = new ArrayList<>(relTypes.size());
                for (Pair<RelationshipType, Direction> p : parsedTypes) {
                    String name = p.first().name();
                    if (relTypes.contains(name) && node.hasRelationship(p.first(), p.other())) {
                        result.add(name);
                    }
                }
                allResults.add(result);
            }
        }
        return allResults;
    }

    @UserFunction("apoc.node.relationships.degrees")
    @Description("apoc.node.relationships.degrees(node, rel-direction-pattern) - returns a map with rel-pattern, boolean for the given relationship patterns")
    public Map<String,Long> relationshipsDegrees(@Name("node") Node node, @Name(value = "types",defaultValue = "") String types) {
        if (node==null) return null;
        return getDegreees(node, parse(types));
    }
    @UserFunction("apoc.nodes.relationships.degrees")
    @Description("apoc.nodes.relationships.degrees(node, rel-direction-pattern) - returns a map with rel-pattern, boolean for the given relationship patterns")
    public List<Map<String,Long>> nodesRelationshipsDegrees(@Name("nodes") List<Node> nodes, @Name(value = "types",defaultValue = "") String types) {
        if (nodes == null) return null;
        if (nodes.isEmpty()) return Collections.emptyList();
        List<Pair<RelationshipType, Direction>> parsedTypes = parse(types);
        return nodes.stream().map(n -> getDegreees(n, parsedTypes)).collect(Collectors.toList());
    }

    public Map<String, Long> getDegreees(Node node, List<Pair<RelationshipType, Direction>> parsedTypes) {
        return parsedTypes.stream().collect(toMap(RelationshipTypeAndDirections::format, (p) -> (long)node.getDegree(p.first(),p.other())));
    }

    @UserFunction
    @Description("apoc.nodes.isDense(node) - returns true if it is a dense node")
    public boolean isDense(@Name("node") Node node) {
        try (NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor()) {
            final long id = node.getId();
            ktx.dataRead().singleNode(id, nodeCursor);
            if (nodeCursor.next()) {
                return nodeCursor.isDense();
            } else {
                throw new IllegalArgumentException("node with id " + id + " does not exist.");
            }
        }
    }

    // works in cases when relType is null
    private int getDegreeSafe(Node node, RelationshipType relType, Direction direction) {
        if (relType == null) {
            return node.getDegree(direction);
        }

        return node.getDegree(relType, direction);
    }

}
