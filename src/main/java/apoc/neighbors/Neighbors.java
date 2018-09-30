package apoc.neighbors;

import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.*;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.stream.*;

import static apoc.path.RelationshipTypeAndDirections.parse;

public class Neighbors {

    @Context
    public GraphDatabaseService db;

    @Procedure("apoc.neighbors")
    @Description("apoc.neighbors(node, rel-direction-pattern, distance) - returns distinct nodes of the given relationships in the pattern up to a certain distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<NodeResult> neighbors(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        RoaringBitmap seen = new RoaringBitmap();
        RoaringBitmap nextA = new RoaringBitmap();
        RoaringBitmap nextB = new RoaringBitmap();
        int nodeId = (int) node.getId();
        seen.add(nodeId);
        Iterator<Integer> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                nextB.add((int) r.getOtherNodeId(node.getId()));
            }
        }

        for(int i = 1; i < distance; i++) {
            // next even Hop
            nextB.andNot(seen);
            seen.or(nextB);
            nextA.clear();
            iterator = nextB.iterator();
            while (iterator.hasNext()) {
                nodeId = iterator.next();
                node = db.getNodeById((long) nodeId);
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        nextA.add((int) r.getOtherNodeId((long) nodeId));
                    }
                }
            }

            i++;
            if (i < distance) {
                // next odd Hop
                nextA.andNot(seen);
                seen.or(nextA);
                nextB.clear();
                iterator = nextA.iterator();
                while (iterator.hasNext()) {
                    nodeId = iterator.next();
                    node = db.getNodeById((long) nodeId);
                    for (Pair<RelationshipType, Direction> pair : parse(types)) {
                        for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                            nextB.add((int) r.getOtherNodeId((long) nodeId));
                        }
                    }
                }
            }
        }
        if((distance % 2) == 0) {
            seen.or(nextA);
        } else {
            seen.or(nextB);
        }
        // remove starting node
        seen.remove((int)node.getId());

        return StreamSupport.stream(seen.spliterator(), false).map(x -> new NodeResult(db.getNodeById(x)));
    }

    @Procedure("apoc.neighbors.count")
    @Description("apoc.neighbors.count(node, rel-direction-pattern, distance) - returns distinct count of nodes of the given relationships in the pattern up to a certain distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<LongResult> neighborsCount(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        RoaringBitmap seen = new RoaringBitmap();
        RoaringBitmap nextA = new RoaringBitmap();
        RoaringBitmap nextB = new RoaringBitmap();
        int nodeId = (int) node.getId();
        seen.add(nodeId);
        Iterator<Integer> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                nextB.add((int) r.getOtherNodeId(node.getId()));
            }
        }

        for(int i = 1; i < distance; i++) {
            // next even Hop
            nextB.andNot(seen);
            seen.or(nextB);
            nextA.clear();
            iterator = nextB.iterator();
            while (iterator.hasNext()) {
                nodeId = iterator.next();
                node = db.getNodeById((long) nodeId);
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        nextA.add((int) r.getOtherNodeId(nodeId));
                    }
                }
            }

            i++;
            if (i < distance) {
                // next odd Hop
                nextA.andNot(seen);
                seen.or(nextA);
                nextB.clear();
                iterator = nextA.iterator();
                while (iterator.hasNext()) {
                    nodeId = iterator.next();
                    node = db.getNodeById((long) nodeId);
                    for (Pair<RelationshipType, Direction> pair : parse(types)) {
                        for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                            nextB.add((int) r.getOtherNodeId(nodeId));
                        }
                    }
                }
            }
        }
        if((distance % 2) == 0) {
            seen.or(nextA);
        } else {
            seen.or(nextB);
        }
        // remove starting node
        seen.remove((int)node.getId());

        return Stream.of(new LongResult((long)seen.getCardinality()));
    }

    @Procedure("apoc.neighbors.byhop")
    @Description("apoc.neighbors.byhop(node, rel-direction-pattern, distance) - returns distinct nodes of the given relationships in the pattern at each distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<NodeListResult> neighborsByHop(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        RoaringBitmap[] seen = new RoaringBitmap[distance.intValue()];
        for(int i = 0; i < distance; i++) {
            seen[i] = new RoaringBitmap();
        }
        int nodeId = (int) node.getId();

        Iterator<Integer> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                seen[0].add((int) r.getOtherNodeId(node.getId()));
            }
        }

        for(int i = 1; i < distance; i++) {
            iterator = seen[i-1].iterator();
            while (iterator.hasNext()) {
                node = db.getNodeById((long) iterator.next());
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        seen[i].add((int) r.getEndNodeId());
                    }
                }
            }
            for(int j = 0; j < i; j++){
                seen[i].andNot(seen[j]);
                seen[i].remove(nodeId);
            }
        }

        return Arrays.stream(seen).map(x -> new NodeListResult(
                StreamSupport.stream(x.spliterator(), false)
                        .map(y -> db.getNodeById((long) y))
                        .collect(Collectors.toList())));
    }

    @Procedure("apoc.neighbors.count.byhop")
    @Description("apoc.neighbors.count.byhop(node, rel-direction-pattern, distance) - returns distinct nodes of the given relationships in the pattern at each distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<ListResult> neighborsCountByHop(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        RoaringBitmap[] seen = new RoaringBitmap[distance.intValue()];
        for(int i = 0; i < distance; i++) {
            seen[i] = new RoaringBitmap();
        }
        int nodeId = (int) node.getId();

        Iterator<Integer> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                seen[0].add((int) r.getOtherNodeId(node.getId()));
            }
        }

        for(int i = 1; i < distance; i++) {
            iterator = seen[i-1].iterator();
            while (iterator.hasNext()) {
                node = db.getNodeById((long) iterator.next());
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        seen[i].add((int) r.getEndNodeId());
                    }
                }
            }
            for(int j = 0; j < i; j++){
                seen[i].andNot(seen[j]);
                seen[i].remove(nodeId);
            }
        }

        ArrayList counts = new ArrayList<Integer>();
        for(int i = 0; i < distance; i++) {
            counts.add(seen[i].getCardinality());
        }

        return Stream.of(new ListResult(counts));
    }
}
