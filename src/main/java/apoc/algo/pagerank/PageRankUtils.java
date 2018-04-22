package apoc.algo.pagerank;

public class PageRankUtils
{
    static final int BATCH_SIZE = 100_000;

    public static int toInt( double value )
    {
        return (int) (100_000 * value);
    }

    public static double toFloat( int value )
    {
        return value / 100_000.0;
    }

}
