package apoc.nodes;

import apoc.Pools;
import apoc.create.Create;
import apoc.refactor.util.PropertiesManager;
import apoc.refactor.util.RefactorConfig;
import apoc.result.LongResult;
import apoc.result.NodeResult;
import apoc.result.PathResult;
import apoc.result.RelationshipResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualPath;
import apoc.result.VirtualPathResult;
import apoc.util.Util;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
//import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.storageengine.api.RelationshipSelection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.path.RelationshipTypeAndDirections.format;
import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.refactor.util.RefactorUtil.copyProperties;
import static apoc.util.Util.map;

public class Nodes {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Context
    public Pools pools;
    
    @Procedure
    @Description("CALL apoc.nodes.cycles([nodes], $config) - Detect all path cycles from node list")
    public Stream<PathResult> cycles(@Name("nodes") List<Node> nodes, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        NodesConfig conf = new NodesConfig(config);
        final List<String> types = conf.getRelTypes();
        Stream<Path> paths = nodes.stream().flatMap(start -> {
            boolean allRels = types.isEmpty();
            final RelationshipType[] relTypes = types.stream().map(RelationshipType::withName).toArray(RelationshipType[]::new);
            final Iterable<Relationship> relationships = allRels
                    ? start.getRelationships(Direction.OUTGOING)
                    : start.getRelationships(Direction.OUTGOING, relTypes);

            PathExpanderBuilder expanderBuilder;
            if (allRels) {
                expanderBuilder = PathExpanderBuilder.allTypes(Direction.OUTGOING);
            } else {
                expanderBuilder = PathExpanderBuilder.empty();
                for (RelationshipType relType: relTypes) {
                    expanderBuilder = expanderBuilder.add(relType, Direction.OUTGOING);
                }
            }
            final PathExpander<Path> pathExpander = expanderBuilder.build();

            PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                    new BasicEvaluationContext(tx, db),
                    pathExpander,
                    conf.getMaxDepth());
            Map<Long, List<Long>> dups = new HashMap<>();
            return Iterables.stream(relationships)
                    // to prevent duplicated (start and end nodes with double-rels)
                    .filter(relationship -> {
                        final List<Long> nodeDups = dups.computeIfAbsent(relationship.getStartNodeId(), (key) -> new ArrayList<>());
                        if (nodeDups.contains(relationship.getEndNodeId())) {
                            return false;
                        }
                        nodeDups.add(relationship.getEndNodeId());
                        return true;
                    })
                    .flatMap(relationship -> {
                        final Path path = finder.findSinglePath(relationship.getEndNode(), start);
                        if (path == null) return Stream.empty();
                        VirtualPath virtualPath = new VirtualPath(start);
                        virtualPath.addRel(relationship);
                        for (Relationship relPath : path.relationships()) {
                            virtualPath.addRel(relPath);
                        }
                        return Stream.of(virtualPath);
                    });
        });
        return paths.map(PathResult::new);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.nodes.link([nodes],'REL_TYPE', conf) - creates a linked list of nodes from first to last")
    public void link(@Name("nodes") List<Node> nodes, @Name("type") String type, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        RefactorConfig conf = new RefactorConfig(config);
        Iterator<Node> it = nodes.iterator();
        if (it.hasNext()) {
            RelationshipType relType = RelationshipType.withName(type);
            Node node = it.next();
            while (it.hasNext()) {
                Node next = it.next();
                final boolean createRelationship = !conf.isAvoidDuplicates() || (conf.isAvoidDuplicates() && !connected(node, next, type));
                if (createRelationship) {
                    node.createRelationshipTo(next, relType);
                }
                node = next;
            }
        }
    }

    @Procedure
    @Description("apoc.nodes.get(node|nodes|id|[ids]) - quickly returns all nodes with these ids")
    public Stream<NodeResult> get(@Name("nodes") Object ids) {
        return Util.nodeStream(tx, ids).map(NodeResult::new);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.nodes.delete(node|nodes|id|[ids]) - quickly delete all nodes with these ids")
    public Stream<LongResult> delete(@Name("nodes") Object ids, @Name("batchSize") long batchSize) {
        Iterator<Node> it = Util.nodeStream(tx, ids).iterator();
        long count = 0;
        while (it.hasNext()) {
            final List<Node> batch = Util.take(it, (int)batchSize);
//            count += Util.inTx(api,() -> batch.stream().peek( n -> {n.getRelationships().forEach(Relationship::delete);n.delete();}).count());
            count += Util.inTx(db, pools, (txInThread) -> {txInThread.execute("FOREACH (n in $nodes | DETACH DELETE n)",map("nodes",batch)).close();return batch.size();});
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure
    @Description("apoc.get.rels(rel|id|[ids]) - quickly returns all relationships with these ids")
    public Stream<RelationshipResult> rels(@Name("relationships") Object ids) {
        return Util.relsStream(tx, ids).map(RelationshipResult::new);
    }

    @UserFunction("apoc.node.relationship.exists")
    @Description("apoc.node.relationship.exists(node, rel-direction-pattern) - returns true when the node has the relationships of the pattern")
    public boolean hasRelationship(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types) {
        if (types == null || types.isEmpty()) return node.hasRelationship();
        long id = node.getId();
        try ( NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext())) {

            ktx.dataRead().singleNode(id, nodeCursor);
            nodeCursor.next();
            TokenRead tokenRead = ktx.tokenRead();

            for (Pair<RelationshipType, Direction> pair : parse(types)) {
                int typeId = tokenRead.relationshipType(pair.first().name());
                Direction direction = pair.other();

                int count;
                switch (direction) {
                    case INCOMING:
                        count = org.neo4j.internal.kernel.api.helpers.Nodes.countIncoming(nodeCursor, typeId);
                        break;
                    case OUTGOING:
                        count = org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoing(nodeCursor, typeId);
                        break;
                    case BOTH:
                        count = org.neo4j.internal.kernel.api.helpers.Nodes.countAll(nodeCursor, typeId);
                        break;
                    default:
                        throw new UnsupportedOperationException("invalid direction " + direction);
                }
                if (count > 0) {
                    return true;
                }
            }
        }
        return false;
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

        try (NodeCursor startNodeCursor = cursors.allocateNodeCursor(ktx.cursorContext());
             NodeCursor endNodeCursor = cursors.allocateNodeCursor(ktx.cursorContext())) {

            dataRead.singleNode(startId, startNodeCursor);
            if (!startNodeCursor.next()) {
                throw new IllegalArgumentException("node with id " + startId + " does not exist.");
            }

//            boolean startDense = startNodeCursor.supportsFastDegreeLookup();
            dataRead.singleNode(endId, endNodeCursor);
            if (!endNodeCursor.next()) {
                throw new IllegalArgumentException("node with id " + endId + " does not exist.");
            }
//            boolean endDense = endNodeCursor.supportsFastDegreeLookup();

            return connected(startNodeCursor, endId, typedDirections(tokenRead, pairs, true));


//            if (!startDense) return connected(startNodeCursor, endId, typedDirections(tokenRead, pairs, true));
//            if (!endDense) return connected(endNodeCursor, startId, typedDirections(tokenRead, pairs, false));
//            return connectedDense(startNodeCursor, endNodeCursor, typedDirections(tokenRead, pairs, true));
        }
    }

    @Procedure
    @Description("apoc.nodes.collapse([nodes...],[{properties:'overwrite' or 'discard' or 'combine'}]) yield from, rel, to merge nodes onto first in list")
    public Stream<VirtualPathResult> collapse(@Name("nodes") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
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
        Iterable<Relationship> rels = virtualNode.getRelationships(direction, source.getType());
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
        try (RelationshipTraversalCursor relationship = ktx.cursors().allocateRelationshipTraversalCursor(ktx.cursorContext())) {
            start.relationships(relationship, RelationshipSelection.selection(Direction.BOTH));
            while (relationship.next()) {
                if (relationship.otherNodeReference() ==end) {
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
            read.relationships(node, group, RelationshipSelection.ALL_RELATIONSHIPS, relationship);
            while (relationship.next()) {
                if (relationship.otherNodeReference()==other) {
                    return true;
                }
            }
            return false;
        }
    }

//    private boolean connectedDense(NodeCursor start, NodeCursor end, int[][] typedDirections) {
//        List<Degree> degrees = new ArrayList<>(32);
//
//        Read read = ktx.dataRead();
//
//        try (RelationshipGroupCursor relationshipGroup = ktx.cursors().allocateRelationshipGroupCursor()) {
//            addDegreesForNode(read, start, end, degrees, relationshipGroup, typedDirections);
//            addDegreesForNode(read, end, start, degrees, relationshipGroup, typedDirections);
//        }
//
//
//        Collections.sort(degrees);
//        try (RelationshipTraversalCursor relationship = ktx.cursors().allocateRelationshipTraversalCursor()) {
//            for (Degree degree : degrees) {
//                if (degree.isConnected(ktx.dataRead(), relationship)) return true;
//            }
//            return false;
//        }
//    }
//
//    private void addDegreesForNode(Read dataRead, NodeCursor node, NodeCursor other, List<Degree> degrees, RelationshipGroupCursor relationshipGroup, int[][] typedDirections) {
//        long nodeId = node.nodeReference();
//        long otherId = other.nodeReference();
//
//        dataRead.relationshipGroups(nodeId, node.relationshipGroupReference(), relationshipGroup);
//        while (relationshipGroup.next()) {
//            int type = relationshipGroup.type();
//            if ((typedDirections==null) || (arrayContains(typedDirections[0], type))) {
//                addDegreeWithDirection(degrees, relationshipGroup.outgoingReference(), relationshipGroup.outgoingCount(), nodeId, otherId);
//            }
//
//            if ((typedDirections==null) || (arrayContains(typedDirections[1], type))) {
//                addDegreeWithDirection(degrees, relationshipGroup.incomingReference(), relationshipGroup.incomingCount(), nodeId, otherId);
//            }
//        }
//    }

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

    @UserFunction("apoc.rel.startNode")
    @Description("returns startNode for (virtual) relationships")
    public Node startNode(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getStartNode();
    }

    @UserFunction("apoc.rel.endNode")
    @Description("returns endNode for (virtual) relationships")
    public Node endNode(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getEndNode();
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
        if (thing instanceof Entity) {
            if (keys == null) return ((Entity) thing).getAllProperties();
            return ((Entity) thing).getProperties(keys.toArray(new String[keys.size()]));
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
        if (thing instanceof Entity) {
            return ((Entity) thing).getProperty(key,null);
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
            if (relTypes.contains(name) && node.hasRelationship(p.other(),p.first())) {
                result.add(name);
            }
        }
        return result;
    }

    @UserFunction("apoc.nodes.relationship.types")
    @Description("apoc.nodes.relationship.types(node|nodes|id|[ids], rel-direction-pattern) - returns " +
            "a list of maps where each one has two fields: `node` which is the node subject of the analysis " +
            "and `types` which is a list of distinct relationship types")
    public List<Map<String, Object>> nodesRelationshipTypes(@Name("ids") Object ids, @Name(value = "types",defaultValue = "") String types) {
        if (ids == null) return null;
        return Util.nodeStream(tx, ids)
                .map(node -> {
                    final List<String> relationshipTypes = relationshipTypes(node, types);
                    if (relationshipTypes == null) {
                        // in order to avoid possible NullPointerException because we'll use Collectors#toMap which uses Map#merge
                        return null;
                    }
                    return map("node", node, "types", relationshipTypes);
                })
                .filter(e -> e != null)
                .collect(Collectors.toList());
    }

    @UserFunction("apoc.node.relationships.exist")
    @Description("apoc.node.relationships.exist(node, rel-direction-pattern) - returns a map with rel-pattern, boolean for the given relationship patterns")
    public Map<String,Boolean> relationshipExists(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types) {
        if (node == null || types == null || types.isEmpty()) return null;
        List<String> relTypes = Iterables.asList(Iterables.map(RelationshipType::name, node.getRelationshipTypes()));
        Map<String,Boolean> result =  new HashMap<>();
        for (Pair<RelationshipType, Direction> p : parse(types)) {
            String name = p.first().name();
            boolean hasRelationship = relTypes.contains(name) && node.hasRelationship(p.other(), p.first());
            result.put(format(p), hasRelationship);
        }
        return result;
    }

    @UserFunction("apoc.nodes.relationships.exist")
    @Description("apoc.nodes.relationships.exist(node|nodes|id|[ids], rel-direction-pattern) - returns " +
            "a list of maps where each one has two fields: `node` which is the node subject of the analysis " +
            "and `exists` which is a map with rel-pattern, boolean for the given relationship patterns")
    public List<Map<String, Object>> nodesRelationshipExists(@Name("ids") Object ids, @Name(value = "types", defaultValue = "") String types) {
        if (ids == null) return null;
        return Util.nodeStream(tx, ids)
                .map(node -> {
                    final Map<String, Boolean> existsMap = relationshipExists(node, types);
                    if (existsMap == null) {
                        // in order to avoid possible NullPointerException because we'll use Collectors#toMap which uses Map#merge
                        return null;
                    }
                    return map("node", node, "exists", existsMap);
                })
                .filter(e -> e != null)
                .collect(Collectors.toList());
    }

    @UserFunction
    @Description("apoc.nodes.isDense(node) - returns true if it is a dense node")
    public boolean isDense(@Name("node") Node node) {
        try (NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext())) {
            final long id = node.getId();
            ktx.dataRead().singleNode(id, nodeCursor);
            if (nodeCursor.next()) {
                return nodeCursor.supportsFastDegreeLookup();
            } else {
                throw new IllegalArgumentException("node with id " + id + " does not exist.");
            }
        }
    }

    @UserFunction("apoc.any.isDeleted")
    @Description("returns boolean value for nodes and rele existance")
    public boolean isDeleted(@Name("thing") Object thing) {
        if (thing == null) return true;
        final String query;
        if (thing instanceof Node) {
            query = "MATCH (n) WHERE ID(n) = $id RETURN COUNT(n) = 1 AS exists";
        }
        else if (thing instanceof Relationship){
            query = "MATCH ()-[r]->() WHERE ID(r) = $id RETURN COUNT(r) = 1 AS exists";
        }
        else {
            throw new IllegalArgumentException("expected Node or Relationship but was " + thing.getClass().getSimpleName());
        }
        return !(boolean) tx.execute(query, Map.of("id",((Entity)thing).getId())).next().get("exists");
    }

    // works in cases when relType is null
    private int getDegreeSafe(Node node, RelationshipType relType, Direction direction) {
        if (relType == null) {
            return node.getDegree(direction);
        }

        return node.getDegree(relType, direction);
    }

}
