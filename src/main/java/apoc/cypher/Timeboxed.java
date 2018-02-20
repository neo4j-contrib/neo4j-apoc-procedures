package apoc.cypher;

import apoc.Pools;
import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author mh
 * @since 20.02.18
 */
public class Timeboxed {


    @Context
    public GraphDatabaseService db;
    @Context
    public KernelTransaction tx;

    @Context
    public Log log;

    @Procedure
    @Description("apoc.cypher.runTimeboxed('cypherStatement',{params}, timeout) - abort statement after timeout ms if not finished")
    public Stream<MapResult> runTimeboxed(@Name("cypher") String cypher, @Name("params") Map<String, Object> params, @Name("timeout") long timeout) {

        Pools.SCHEDULED.schedule(() -> {
            String txString = tx == null ? "<null>" : tx.toString();
            log.warn("marking " + txString + " for termination");
            tx.markForTermination(Status.Transaction.Terminated);
        }, timeout, MILLISECONDS);

        Result result = db.execute(cypher, params == null ? Collections.EMPTY_MAP : params);
        return result.stream().map(MapResult::new);
    }
}
