package apoc.cypher;

import apoc.Pools;
import apoc.result.MapResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author mh
 * @since 20.02.18
 */
public class Timeboxed {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private static Map<String,Object> POISON = Collections.singletonMap("__magic", "POISON");

    @Procedure
    @Description("apoc.cypher.runTimeboxed('cypherStatement',{params}, timeout) - abort statement after timeout ms if not finished")
    public Stream<MapResult> runTimeboxed(@Name("cypher") String cypher, @Name("params") Map<String, Object> params, @Name("timeout") long timeout) {

        final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(100);
        final AtomicReference<Transaction> txAtomic = new AtomicReference<>();

        // run query to be timeboxed in a separate thread to enable proper tx termination
        // if we'd run this in current thread, a tx.terminate would kill the transaction the procedure call uses itself.
        Pools.DEFAULT.submit(() -> {
            try (Transaction tx = db.beginTx()) {
                txAtomic.set(tx);
                Result result = db.execute(cypher, params == null ? Collections.EMPTY_MAP : params);
                while (result.hasNext()) {
                    final Map<String, Object> map = result.next();
                    queue.put(map);
                }
                queue.put(POISON);
                tx.success();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (TransactionTerminatedException e) {
                log.warn("query " + cypher + " has been terminated");
            } finally {
                txAtomic.set(null);
            }
        });

        //
        Pools.SCHEDULED.schedule(() -> {
            Transaction tx = txAtomic.get();
            if (tx==null) {
                log.error("oops, tx is null, maybe other thread already finished?");
            } else {
                tx.terminate();
                try {
                    queue.put(POISON);
                    log.warn("terminating transaction, putting POISON onto queue");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, timeout, MILLISECONDS);

        // consume the blocking queue using a custom iterator finishing upon POISON
        Iterator<Map<String,Object>> queueConsumer = new Iterator<Map<String, Object>>() {
            Map<String,Object> nextElement = null;
            boolean hasPoisoned = false;

            @Override
            public boolean hasNext() {
                if (hasPoisoned) {
                    return false;
                } else {
                    try {
                        nextElement = queue.take();
                        hasPoisoned = POISON.equals(nextElement);
                        return !hasPoisoned;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public Map<String, Object> next() {
                return nextElement;
            }
        };
        return StreamSupport
                .stream( Spliterators.spliteratorUnknownSize(queueConsumer, Spliterator.ORDERED), false)
                .map(MapResult::new);
    }
}
