package apoc.neighbors;

import apoc.result.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.*;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.*;
import java.util.stream.*;

import static apoc.path.RelationshipTypeAndDirections.parse;

public class NeighborsLarge {

    @Context
    public GraphDatabaseService db;

    @Procedure("apoc.neighbors.large")
    @Description("apoc.neighbors.large(node, rel-direction-pattern, distance) - returns distinct nodes of the given relationships in the pattern up to a certain distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<NodeResult> neighbors(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap seen = new Roaring64NavigableMap();
        Roaring64NavigableMap nextA = new Roaring64NavigableMap();
        Roaring64NavigableMap nextB = new Roaring64NavigableMap();
        long nodeId = node.getId();
        seen.addLong(nodeId);
        Iterator<Long> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                nextB.addLong(r.getOtherNodeId(nodeId));
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
                node = db.getNodeById(nodeId);
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        nextA.add((r.getOtherNodeId(nodeId)));
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
                    node = db.getNodeById(nodeId);
                    for (Pair<RelationshipType, Direction> pair : parse(types)) {
                        for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                            nextB.add(r.getOtherNodeId(nodeId));
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
        seen.removeLong(node.getId());

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(seen.iterator(), Spliterator.SORTED), false)
                .map(x -> new NodeResult(db.getNodeById(x)));
    }

    @Procedure("apoc.neighbors.large.count")
    @Description("apoc.neighbors.large.count(node, rel-direction-pattern, distance) - returns distinct count of nodes of the given relationships in the pattern up to a certain distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<LongResult> neighborsCount(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap seen = new Roaring64NavigableMap();
        Roaring64NavigableMap nextA = new Roaring64NavigableMap();
        Roaring64NavigableMap nextB = new Roaring64NavigableMap();
        long nodeId = node.getId();
        seen.add(nodeId);
        Iterator<Long> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                nextB.add(r.getOtherNodeId(nodeId));
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
                node = db.getNodeById(nodeId);
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        nextA.add(r.getOtherNodeId(nodeId));
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
                    node = db.getNodeById(nodeId);
                    for (Pair<RelationshipType, Direction> pair : parse(types)) {
                        for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                            nextB.add(r.getOtherNodeId(nodeId));
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
        seen.removeLong(node.getId());

        return Stream.of(new LongResult(seen.getLongCardinality()));
    }

    @Procedure("apoc.neighbors.large.byhop")
    @Description("apoc.neighbors.large.byhop(node, rel-direction-pattern, distance) - returns distinct nodes of the given relationships in the pattern at each distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<NodeListResult> neighborsByHop(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for(int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        long nodeId = node.getId();

        Iterator<Long> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                seen[0].add(r.getOtherNodeId(nodeId));
            }
        }

        for(int i = 1; i < distance; i++) {
            iterator = seen[i-1].iterator();
            while (iterator.hasNext()) {
                node = db.getNodeById(iterator.next());
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        seen[i].add(r.getEndNodeId());
                    }
                }
            }
            for(int j = 0; j < i; j++){
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        return Arrays.stream(seen).map(x -> new NodeListResult(
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(x.iterator(), Spliterator.SORTED), false)
                        .map(y -> db.getNodeById((long) y))
                        .collect(Collectors.toList())));
    }

    @Procedure("apoc.neighbors.large.count.byhop")
    @Description("apoc.neighbors.large.count.byhop(node, rel-direction-pattern, distance) - returns distinct nodes of the given relationships in the pattern at each distance, can use '>' or '<' for all outgoing or incoming relationships")
    public Stream<ListResult> neighborsCountByHop(@Name("node") Node node, @Name(value = "types", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for(int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        long nodeId = node.getId();

        Iterator<Long> iterator;

        // First Hop
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                seen[0].add(r.getOtherNodeId(nodeId));
            }
        }

        for(int i = 1; i < distance; i++) {
            iterator = seen[i-1].iterator();
            while (iterator.hasNext()) {
                node = db.getNodeById(iterator.next());
                for (Pair<RelationshipType, Direction> pair : parse(types)) {
                    for (Relationship r : node.getRelationships(pair.first(), pair.other())) {
                        seen[i].add(r.getEndNodeId());
                    }
                }
            }
            for(int j = 0; j < i; j++){
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        ArrayList counts = new ArrayList<Long>();
        for(int i = 0; i < distance; i++) {
            counts.add(seen[i].getLongCardinality());
        }

        return Stream.of(new ListResult(counts));
    }
}
