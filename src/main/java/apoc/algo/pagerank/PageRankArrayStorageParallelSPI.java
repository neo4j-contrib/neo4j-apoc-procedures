package apoc.algo.pagerank;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntPredicate;

import apoc.algo.algorithms.AlgoUtils;
import apoc.algo.algorithms.AlgorithmInterface;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static apoc.algo.pagerank.PageRankArrayStorageParallelCypher.WRITE_BATCH;
import static apoc.algo.pagerank.PageRankUtils.*;

public class PageRankArrayStorageParallelSPI implements PageRank, AlgorithmInterface
{
    public static final int ONE_MINUS_ALPHA_INT = toInt( ONE_MINUS_ALPHA );
    private final GraphDatabaseAPI db;
    private final int nodeCount;
    private final ExecutorService pool;
    private final int relCount;
    private AtomicIntegerArray dst;

    private PageRankStatistics stats = new PageRankStatistics();

    public PageRankArrayStorageParallelSPI(
            GraphDatabaseService db,
            ExecutorService pool )
    {
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.nodeCount = new NodeCounter().getNodeCount( db );
        this.relCount = new NodeCounter().getRelationshipCount( db );
    }

    @Override
    public void compute(
            int iterations,
            RelationshipType... relationshipTypes )
    {
        stats.iterations = iterations;
        long start = System.currentTimeMillis();
        final int[] src = new int[nodeCount];
        dst = new AtomicIntegerArray( nodeCount );
        final int[] degrees = computeDegrees(ctx(this.db));
        stats.readNodeMillis = System.currentTimeMillis() - start;
        stats.nodes = nodeCount;
        start = System.currentTimeMillis();

        final ReadOperations ops = ctx(this.db).get().readOperations();
        final IntPredicate isValidRelationshipType = relationshipTypeArrayToIntPredicate( ops, relationshipTypes );
        final RelationshipVisitor<RuntimeException> visitor = isValidRelationshipType == null ?
                ( relId, relTypeId, startNode, endNode ) -> dst.addAndGet((int) endNode, src[(int) startNode] ) :
                ( relId, relTypeId, startNode, endNode ) -> {
                    if ( isValidRelationshipType.test( relTypeId ) )
                    {
                        dst.addAndGet( (int) endNode, src[(int) startNode] );
                    }
                };

        PrimitiveLongIterator rels = ops.relationshipsGetAll();
        List<BatchRunnable> runners = prepareOperations(rels, relCount, db, (readOps, id) -> readOps.relationshipVisit(id, visitor));
        stats.readRelationshipMillis = System.currentTimeMillis() - start;
        stats.relationships = relCount;

        start = System.currentTimeMillis();
        for ( int iteration = 0; iteration < iterations; iteration++ )
        {
            startIteration( src, dst, degrees );
            //PrimitiveLongIterator rels = ops.relationshipsGetAll();
            //runOperations( pool, rels, relCount, ctx, (readOps, id) -> readOps.relationshipVisit( id, visitor ) );
            runOperations(pool, runners);
        }
        stats.computeMillis = System.currentTimeMillis() - start;
    }

    private IntPredicate relationshipTypeArrayToIntPredicate(
            ReadOperations ops,
            RelationshipType... relationshipTypes )
    {
        if ( 0 == relationshipTypes.length )
        {
            return null;
        }
        else
        {
            BitSet relationshipTypeSet = new BitSet( relationshipTypes.length );
            for ( RelationshipType relationshipType : relationshipTypes )
            {
                int relTypeId = ops.relationshipTypeGetForName(relationshipType.name());
                if (relTypeId >= 0) relationshipTypeSet.set(relTypeId);
            }
            return relationshipTypeSet::get;
        }
    }

    private void startIteration( int[] src, AtomicIntegerArray dst, int[] degrees )
    {
        for ( int node = 0; node < this.nodeCount; node++ )
        {
            if ( degrees[node] == -1 )
            { continue; }
            src[node] = toInt( ALPHA * toFloat( dst.getAndSet( node, ONE_MINUS_ALPHA_INT ) ) / degrees[node] );

        }
    }

    private int[] computeDegrees( final ThreadToStatementContextBridge ctx )
    {
        final int[] degree = new int[nodeCount];
        Arrays.fill( degree, -1 );
        PrimitiveLongIterator it = ctx.get().readOperations().nodesGetAll();
        int totalCount = nodeCount;
        runOperations( pool, it, totalCount, db , (ops,id) -> degree[id] = ops.nodeGetDegree( id, Direction.OUTGOING ) );
        return degree;
    }

    public double getResult( long node )
    {
        return dst != null ? toFloat( dst.get( (int) node ) ) : 0;
    }


    public long numberOfNodes()
    {
        return nodeCount;
    }

    public String getPropertyName()
    {
        return "pagerank";
    }

    @Override
    public PageRankStatistics getStatistics() {
        return stats;
    }

    @Override
    public long getMappedNode(int algoId) {
        return (int) algoId;
    }

    public void writeResultsToDB() {
        stats.write = true;
        long before = System.currentTimeMillis();
        AlgoUtils.writeBackResults(pool, db, this, WRITE_BATCH);
        stats.write = true;
        stats.writeMillis = System.currentTimeMillis() - before;
        stats.property = getPropertyName();
    }

}
