package apoc.stats;

import apoc.Pools;
import apoc.path.RelationshipTypeAndDirections;
import apoc.util.kernel.MultiThreadedGlobalGraphOperations;
import org.HdrHistogram.AtomicHistogram;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.token.api.NamedToken;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;

/**
 * @author mh
 * @since 07.08.17
 */
public class DegreeDistribution {

    private static final int BATCHSIZE = 10_000;

    public static class DegreeStats {
        public final String typeName;
        public final long total;
        private final int type;
        private final Direction direction;
        private transient AtomicHistogram histogram;

        public void computeDegree(NodeCursor nodeCursor, CursorFactory cursors) {
            int degree = DegreeUtil.degree(nodeCursor, cursors, type, direction);
            record(degree);
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

    @Context
    public KernelTransaction tx;

    @Context
    public Pools pools;

    @Procedure
    public Stream<DegreeStats.Result> degrees(@Name(value = "types", defaultValue = "") String types) {
        List<DegreeStats> stats = prepareStats(types);

        MultiThreadedGlobalGraphOperations.forAllNodes(db, pools.getDefaultExecutorService(), BATCHSIZE,
                (ktx,nodeCursor)-> stats.forEach((s) -> s.computeDegree(nodeCursor, ktx.cursors()))
        );
        return stats.stream().map(DegreeStats::done);
    }

    public List<DegreeStats> prepareStats(String types) {
        List<DegreeStats> stats = new ArrayList<>();
        TokenRead tokenRead = tx.tokenRead();
        Read read = tx.dataRead();
        if ("*".equals(types)) {
            Iterator<NamedToken> tokens = tokenRead.relationshipTypesGetAllTokens();
            while (tokens.hasNext()) {
                NamedToken token = tokens.next();
                long total = read.countsForRelationship(ANY_LABEL, token.id(), ANY_LABEL);
                stats.add(new DegreeStats(token.name(), token.id(), Direction.OUTGOING, total));
                stats.add(new DegreeStats(token.name(), token.id(), Direction.INCOMING, total));
            }
            return stats;
        }
        List<Pair<RelationshipType, Direction>> pairs = RelationshipTypeAndDirections.parse(types);
        for (Pair<RelationshipType, Direction> pair : pairs) {
            String typeName = pair.first() == null ? null : pair.first().name();
            int type = typeName == null ? ANY_RELATIONSHIP_TYPE : tokenRead.relationshipType(typeName);
            long total = read.countsForRelationship(ANY_LABEL, type, ANY_LABEL);
            stats.add(new DegreeStats(typeName, type, pair.other(), total));
        }
        return stats;
    }
}
