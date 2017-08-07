package apoc.stats;

import apoc.Pools;
import apoc.path.RelationshipTypeAndDirections;
import apoc.util.Util;
import org.HdrHistogram.AtomicHistogram;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.Token;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;

/**
 * @author mh
 * @since 07.08.17
 */
public class DegreeDistribution {

    public static class DegreeStats {
        public final String typeName;
        public final long total;
        private final int type;
        private final Direction direction;
        private transient AtomicHistogram histogram;

        public void computeDegree(ReadOperations ops, long id) {
            try {
                int degree = type == ANY_RELATIONSHIP_TYPE ?
                        ops.nodeGetDegree(id, direction) :
                        ops.nodeGetDegree(id, direction, type);
                record(degree);
            } catch (EntityNotFoundException e) {
                // ignore
            }
        }

        public static class Result {
            public String type;
            public String direction;
            public long total;
            public long p50, p75, p90, p95, p99, p999, max, min;
            public double mean;
        }

        public DegreeStats(String typeName, int type, Direction direction, long total) {
            this.typeName = typeName == null ? null : typeName;
            this.type = type;
            this.direction = direction;
            this.total = total;
            this.histogram = new AtomicHistogram(total,3);
        }
        public void record(long value) {
            histogram.recordValue(value);
        }
        public Result done() {
            Result result = new Result();
            result.type = typeName;
            result.direction = direction.name();
            result.total = total;
            result.max = histogram.getMaxValue();
            result.min = histogram.getMinValue();
            result.mean = histogram.getMean();
            result.p50 = histogram.getValueAtPercentile(50);
            result.p75 = histogram.getValueAtPercentile(75);
            result.p90 = histogram.getValueAtPercentile(90);
            result.p95 = histogram.getValueAtPercentile(95);
            result.p99 = histogram.getValueAtPercentile(99);
            result.p999 = histogram.getValueAtPercentile(99.9);
            this.histogram.reset();
            this.histogram = null;
            return result;
        }
    }

    @Context
    public GraphDatabaseAPI db;

    @Procedure
    public Stream<DegreeStats.Result> degrees(@Name(value = "types", defaultValue = "") String types) {
        return Util.withStatement(db, (st, ops) -> {
            List<DegreeStats> stats = prepareStats(types, ops);
            PrimitiveLongIterator it = ops.nodesGetAll();
            List<Future> futures = new ArrayList<>();
            do {
                long[] batch = Util.takeIds(it, 10000);
                futures.add(Util.inTxFuture(Pools.DEFAULT, db, (stmt, ro) -> computeDegree(ro, stats, batch)));
                Util.removeFinished(futures);
            } while (it.hasNext());
            Util.waitForFutures(futures);
            return stats.stream().map(DegreeStats::done);
        });
    }

    public int computeDegree(ReadOperations ops, List<DegreeStats> stats, long[] batch) {
        for (long id : batch) {
            stats.forEach((s) -> s.computeDegree(ops, id));
        }
        return batch.length;
    }

    public List<DegreeStats> prepareStats(@Name(value = "types", defaultValue = "") String types, ReadOperations ops) {
        List<DegreeStats> stats = new ArrayList<>();
        if ("*".equals(types)) {
            Iterator<Token> tokens = ops.relationshipTypesGetAllTokens();
            while (tokens.hasNext()) {
                Token token = tokens.next();
                long total = ops.countsForRelationship(ANY_LABEL, token.id(), ANY_LABEL);
                stats.add(new DegreeStats(token.name(), token.id(), Direction.OUTGOING, total));
                stats.add(new DegreeStats(token.name(), token.id(), Direction.INCOMING, total));
            }
            return stats;
        }
        List<Pair<RelationshipType, Direction>> pairs = RelationshipTypeAndDirections.parse(types);
        for (Pair<RelationshipType, Direction> pair : pairs) {
            String typeName = pair.first() == null ? null : pair.first().name();
            int type = typeName == null ? ANY_RELATIONSHIP_TYPE : ops.relationshipTypeGetForName(typeName);
            long total = ops.countsForRelationship(ANY_LABEL, type, ANY_LABEL);
            stats.add(new DegreeStats(typeName, type, pair.other(), total));
        }
        return stats;
    }
}
