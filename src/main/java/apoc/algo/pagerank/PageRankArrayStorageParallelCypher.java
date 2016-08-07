package apoc.algo.pagerank;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static apoc.algo.pagerank.PageRankUtils.toFloat;
import static apoc.algo.pagerank.PageRankUtils.toInt;
import static java.lang.Math.toIntExact;

public class PageRankArrayStorageParallelCypher implements PageRank
{
    public static final int ONE_MINUS_ALPHA_INT = toInt( ONE_MINUS_ALPHA );
    public final int BATCH_SIZE = 100_000;
    private final GraphDatabaseAPI db;
    private final ExecutorService pool;
    private String cypher = "";

    /*
        Integer key = 16 bytes + 4 bytes for map reference
        Integer value = 16 bytes.
        Thus single object in this map = 16 + 16 + 4 bytes = 36 bytes.

        1. TODO:
        For a single map, we can use sorted int [] as keys and another int [] to hold the value
        at the cost of CPU consumption.

        Map: Earlier Map[1234] = 12; Search for 1234 would be O(1)
        int[] approach: key[x] = 1234; val[x] = 12; Search for x would be O(log(n))

        Memory consumption in array approach : 2 ints: 4 + 4  = 8.
        Around 4.5x memory improvement.

        So we can have 6 arrays here to do the same job as following 3 at the cost of computation and save 4.5x
        on memory. Good scaling perf.

        2. TODO:
        Run this in parallel using ExecutorService.
        Sort and divide into (number of threads) chunks and process it in parallel.
        We *might* need to store the relationships too for that purpose.
     */
    private Map<Integer, Integer> sourceDegree;
    private Map<Integer, Integer> pageRanks;
    private Map<Integer, Integer> previousPageRank;

    public PageRankArrayStorageParallelCypher(
            GraphDatabaseService db,
            ExecutorService pool,
            String cypher)
    {
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.cypher = cypher;
        readDataIntoArray(cypher);
    }

    private void readDataIntoArray(String cypher) {
        sourceDegree = new HashMap<>();
        pageRanks = new HashMap<>();
        previousPageRank = new HashMap<>();

        Result result = db.execute(cypher);
        while(result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = ((Long) res.get("source")).intValue();
            int target = ((Long) res.get("target")).intValue();
            int weight = ((Long) res.getOrDefault("weight", 1)).intValue();

            if (sourceDegree.containsKey(source)) {
                int storedDegree = sourceDegree.get(source);
                sourceDegree.put(source, storedDegree + weight);
            } else {
                sourceDegree.put(source, weight);
            }

            // This would be the union of target as well as source nodes.
            // Because this will act as a placeholder for intermediate results of both.
            previousPageRank.put(source, 0);
            previousPageRank.put(target, 0);

            pageRanks.put(target, 0);
        }
        result.close();
    }

    private void printMap(Map<Integer, Integer> map) {
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    @Override
    public void compute(
            int iterations,
            RelationshipType... relationshipTypes)
    {
        for (int iteration = 0; iteration < iterations; iteration++) {
            startIteration();
            iterate(this.cypher);
        }
    }

    private void iterate(String cypher) {
        Result result = db.execute(cypher);

        while (result.hasNext()) {
            Map<String, Object> res = result.next();
            int source = toIntExact((Long) res.get("source"));
            int target = toIntExact((Long) res.get("target"));
            int weight = toIntExact((Long) res.getOrDefault("weight", 1));
            int oldValue = pageRanks.getOrDefault(target, 0);
            int newValue =  oldValue + weight * previousPageRank.getOrDefault(source, 0);
            pageRanks.put(target, newValue);
        }
        result.close();
    }


    private void startIteration()
    {
        for (Map.Entry<Integer, Integer> entry : previousPageRank.entrySet()) {
            int node = entry.getKey();
            int degree = sourceDegree.getOrDefault(node, 0);

            if (degree == 0) {
                continue;
            }
            int prevRank = pageRanks.getOrDefault(node, 0);
            previousPageRank.put(node, toInt(ALPHA * toFloat(prevRank) / degree));
            pageRanks.put(node, ONE_MINUS_ALPHA_INT);

        }
    }

    public double getResult(long node)
    {
        double val = 0;
        if (pageRanks.containsKey((int)node)) {
            val = toFloat(pageRanks.get((int)node));
        }
        return val;
    }

    public String getPropertyName()
    {
        return "pagerank";
    }

    public long numberOfNodes() {
        return previousPageRank.size();
    };

}
