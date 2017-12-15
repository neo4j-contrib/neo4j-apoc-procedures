package apoc.nodes;

import apoc.result.LongResult;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.procedure.*;
import org.neo4j.storageengine.api.Token;

import java.util.*;
import java.util.stream.Stream;

import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.util.Util.map;

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
    public boolean hasRelationship(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types) throws EntityNotFoundException {
        if (types == null || types.isEmpty()) return node.hasRelationship();
        long id = node.getId();
        try (Statement stmt = ktx.acquireStatement()) {
            ReadOperations ops = stmt.readOperations();
            boolean dense = ops.nodeIsDense(id);
            for (Pair<RelationshipType, Direction> pair : parse(types)) {
            int typeId = ops.relationshipTypeGetForName(pair.first().name());
            Direction direction = pair.other();
            boolean hasRelationship = (dense) ?
                    ops.nodeGetDegree(id,direction,typeId) > 0 :
                    ops.nodeGetRelationships(id, direction,new int[] {typeId}).hasNext();
            if (hasRelationship) return true;
            }
        }

        return false;
    }

    @UserFunction("apoc.nodes.connected")
    @Description("apoc.nodes.connected(start, end, rel-direction-pattern) - returns true when the node is connected to the other node, optimized for dense nodes")
    public boolean connected(@Name("start") Node start, @Name("start") Node end, @Name(value = "types", defaultValue = "") String types) throws EntityNotFoundException {
        if (start == null || end == null) return false;
        if (start.equals(end)) return true;

        long startId = start.getId();
        long endId = end.getId();
        List<Pair<RelationshipType, Direction>> pairs = (types == null || types.isEmpty()) ? null : parse(types);

        try (Statement stmt = ktx.acquireStatement()) {
            ReadOperations ops = stmt.readOperations();

            boolean startDense = ops.nodeIsDense(startId);
            boolean endDense = ops.nodeIsDense(endId);

            if (!startDense) return connected(ops, startId, endId, typedDirections(ops, pairs, true));
            if (!endDense) return connected(ops, endId, startId, typedDirections(ops, pairs, false));
            return connectedDense(ops, startId, endId, pairs);
        }
    }

    private boolean connected(ReadOperations ops, long start, long end, int[][] typedDirections) throws EntityNotFoundException {
        MatchingRelationshipVisitor matcher = (typedDirections == null) ?
                new MatchingRelationshipAllVisitor(end) :
                new MatchingRelationshipTypesDirectionVisitor(typedDirections,end);
        return checkRelationships(ops.nodeGetRelationships(start, Direction.BOTH), matcher);
    }

    private int[][] typedDirections(ReadOperations ops, List<Pair<RelationshipType, Direction>> pairs, boolean outgoing) {
        if (pairs==null) return null;
        int from=0;int to=0, both = 0;
        int[][] result = new int[Direction.values().length][pairs.size()];
        int outIdx = Direction.OUTGOING.ordinal();
        int inIdx = Direction.INCOMING.ordinal();
        int bothIdx = Direction.BOTH.ordinal();
        for (Pair<RelationshipType, Direction> pair : pairs) {
            int type = ops.relationshipTypeGetForName(pair.first().name());
            if (type == -1) continue;
            if (pair.other() != Direction.INCOMING) {
                result[outIdx][from++]= type;
            }
            if (pair.other() != Direction.OUTGOING) {
                result[inIdx][to++]= type;
            }
            result[bothIdx][both++]= type;
        }
        result[outIdx] = Arrays.copyOf(result[outIdx], from);
        result[inIdx] = Arrays.copyOf(result[inIdx], to);
        result[bothIdx] = Arrays.copyOf(result[bothIdx], both);
        if (!outgoing) {
            int[] tmp = result[outIdx];
            result[outIdx] = result[inIdx];
            result[inIdx] = tmp;
        }
        return result;
    }

    static class Degree implements Comparable<Degree> {
        public final long node;
        public final int type;
        public final Direction direction;
        public final int degree;
        public final long other;

        public Degree(long node, int type, Direction direction, int degree, long other) {
            this.node = node;
            this.type = type;
            this.direction = direction;
            this.degree = degree;
            this.other = other;
        }

        @Override
        public int compareTo(Degree o) {
            return Integer.compare(degree, o.degree);
        }

        public boolean isConnected(ReadOperations ops, MatchingRelationshipVisitor matcher) throws EntityNotFoundException {
            if (degree == 0) return false;
            if (other == node) return true;
            matcher.reset();
            int[] types = {type};
            if (direction == Direction.OUTGOING) return checkRelationships(ops.nodeGetRelationships(node, Direction.OUTGOING, types), matcher);
            if (direction == Direction.INCOMING) return checkRelationships(ops.nodeGetRelationships(node, Direction.INCOMING, types), matcher);
            return checkRelationships(ops.nodeGetRelationships(node, Direction.BOTH, types), matcher);
        }
    }

    public static boolean checkRelationships(RelationshipIterator it, MatchingRelationshipVisitor matcher) {
        long id;
        while (it.hasNext()) {
            id = it.next();
            it.relationshipVisit(id, matcher);
            if (matcher.matched()) return true;
        }
        return false;
    }


    private boolean connectedDense(ReadOperations ops, long start, long end, List<Pair<RelationshipType, Direction>> pairs) throws EntityNotFoundException {
        List<Degree> degrees = new ArrayList<>(32);

        // direction -> [typeIdOut,typeIdOut,...typeIdIn,typeIdIn]
        int[][] typedDirectionsOut = typedDirections(ops, pairs,true);
        if (pairs == null) {
            int totalTypes = ops.relationshipTypeCount();
            BitSet given = new BitSet(totalTypes);

            Iterator<Token> tokens = ops.relationshipTypesGetAllTokens();
            while (tokens.hasNext()) {
                given.set(tokens.next().id());
            }

            BitSet types = typesOf(ops, totalTypes, given, start);
            types.and(typesOf(ops, totalTypes, given, end));

            if (types.isEmpty()) return false;

            int length = types.length();
            for (int type = 0; type < length; type++) {
                if (types.get(type)) {
                    addSmallestDegree(ops, degrees, start, end, type, Direction.OUTGOING);
                    addSmallestDegree(ops, degrees, start, end, type, Direction.INCOMING);
                }
            }
        } else {
            int totalTypes = typedDirectionsOut.length;
            BitSet given = new BitSet(totalTypes);
            for (int type : typedDirectionsOut[Direction.BOTH.ordinal()]) {
                given.set(type);
            }
            BitSet types = typesOf(ops, totalTypes, given, start);
            types.and(typesOf(ops, totalTypes, given, end));

            for (int type : typedDirectionsOut[Direction.OUTGOING.ordinal()]) {
                addSmallestDegree(ops, degrees, start, end, type, Direction.OUTGOING);
            }
            for (int type : typedDirectionsOut[Direction.INCOMING.ordinal()]) {
                addSmallestDegree(ops, degrees, start, end, type, Direction.INCOMING);
            }
        }
        Collections.sort(degrees);
        MatchingRelationshipVisitor startMatcher = new MatchingRelationshipAllVisitor(start);
        MatchingRelationshipVisitor endMatcher = new MatchingRelationshipAllVisitor(end);
        for (Degree degree : degrees) {
            MatchingRelationshipVisitor matcher = degree.other == start ? startMatcher : endMatcher;
            if (degree.isConnected(ops,matcher)) return true;
        }
        return false;
    }

    private BitSet typesOf(ReadOperations ops, int totalTypes, BitSet given, long node) throws EntityNotFoundException {
        BitSet types = new BitSet(totalTypes);
        PrimitiveIntIterator it = ops.nodeGetRelationshipTypes(node);
        while (it.hasNext()) {
            int type = it.next();
            if (given.get(type)) types.set(type);
        }
        return types;
    }

    private void addSmallestDegree(ReadOperations ops, List<Degree> degrees, long start, long end, int type, Direction direction) throws EntityNotFoundException {
        int startDegree = ops.nodeGetDegree(start,direction,type);
        if (startDegree == 0) return;
        Direction reverse = direction.reverse();
        int endDegree = ops.nodeGetDegree(end,reverse,type);
        if (endDegree == 0) return;
        if (startDegree < endDegree) degrees.add(new Degree(start, type, direction, startDegree, end));
        else degrees.add(new Degree(end, type, reverse, endDegree, start));
    }

    private <T> List<T> asList(Iterable<T> types) {
        return (types instanceof List) ? (List<T>) types : Iterables.asList(types);
    }

    @UserFunction("apoc.node.degree")
    @Description("apoc.node.degree(node, rel-direction-pattern) - returns total degrees of the given relationships in the pattern, can use '>' or '<' for all outgoing or incoming relationships")
    public long degree(@Name("node") Node node, @Name(value = "types",defaultValue = "") String types) throws EntityNotFoundException {
        if (types==null || types.isEmpty()) return node.getDegree();
        long degree = 0;
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            degree += getDegreeSafe(node, pair.first(), pair.other());
        }
        return degree;
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

    @UserFunction
    @Description("apoc.nodes.isDense(node) - returns true if it is a dense node")
    public boolean isDense(@Name("node") Node node) {
        try (Statement stmt = ktx.acquireStatement()) {
            return isDense(stmt.readOperations(), node);
        }
    }

    private boolean isDense(ReadOperations ops, Node n) {
        try {
            return ops.nodeIsDense(n.getId());
        } catch (EntityNotFoundException e) {
            return false;
        }
    }

    // works in cases when relType is null
    private int getDegreeSafe(Node node, RelationshipType relType, Direction direction) {
        if (relType == null) {
            return node.getDegree(direction);
        }

        return node.getDegree(relType, direction);
    }

    private int getDegreeSafe(ReadOperations ops, long id, Direction direction, int typeId) throws EntityNotFoundException {
        if (typeId != -1) {
            return ops.nodeGetDegree(id, direction, typeId);
        }

        return ops.nodeGetDegree(id, direction);
    }

    public static class DenseNodeResult {
        public final Node node;
        public final boolean dense;

        public DenseNodeResult(Node node, boolean dense) {
            this.node = node;
            this.dense = dense;
        }
    }

    interface MatchingRelationshipVisitor extends RelationshipVisitor<RuntimeException> {
        boolean matched();

        void reset();
    }

    private static class MatchingRelationshipAllVisitor implements MatchingRelationshipVisitor {
        final long targetId;
        boolean matched;

        private MatchingRelationshipAllVisitor(long targetId) {
            this.targetId = targetId;
        }

        public void visit(long relationshipId, int typeId, long startNodeId, long endNodeId) throws RuntimeException {
            matched = endNodeId == targetId || startNodeId == targetId;
        }

        @Override
        public boolean matched() {
            return matched;
        }

        @Override
        public void reset() {
            matched = false;
        }
    }
    private static class MatchingRelationshipTypesDirectionVisitor implements MatchingRelationshipVisitor {
        private final int[][] typedDirections;
        final long targetId;
        boolean matched;

        private MatchingRelationshipTypesDirectionVisitor(int[][] typedDirections, long targetId) {
            this.typedDirections = typedDirections;
            this.targetId = targetId;
        }

        public void visit(long relationshipId, int typeId, long startNodeId, long endNodeId) throws RuntimeException {
            matched = false;
            if (endNodeId == targetId) {
                for (int type : typedDirections[Direction.OUTGOING.ordinal()]) if (type == typeId) matched = true;
            }
            if (startNodeId == targetId) {
                for (int type : typedDirections[Direction.INCOMING.ordinal()]) if (type == typeId) matched = true;
            }
        }

        @Override
        public boolean matched() {
            return matched;
        }

        @Override
        public void reset() {
            matched = false;
        }
    }
}
