package apoc.algo.pagerank;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class BatchRunnable implements Runnable, OpsRunner
{
    final long[] ids;
    final ReadOperations ops;
    private final OpsRunner runner;
    int offset = 0;

    public BatchRunnable( ReadOperations ops, PrimitiveLongIterator iterator, int batchSize, OpsRunner runner )
    {
        ids = add( iterator, batchSize );
        this.ops = ops;
        this.runner = runner;
    }

    private long[] add( PrimitiveLongIterator it, int count )
    {
        long[] ids = new long[count];
        while ( count-- > 0 && it.hasNext() )
        {
            ids[offset++] = it.next();
        }
        return ids;
    }

    public void run()
    {
        int notFound = 0;
        for ( int i = 0; i < offset; i++ )
        {
            try
            {
                run( (int) ids[i] );
            }
            catch ( EntityNotFoundException e )
            {
                notFound++;
            }
        }
        if ( notFound > 0 )
        { System.err.println( "Entities not found " + notFound ); }
    }

    @Override
    public void run( int node ) throws EntityNotFoundException
    {
        runner.run( node );
    }
}