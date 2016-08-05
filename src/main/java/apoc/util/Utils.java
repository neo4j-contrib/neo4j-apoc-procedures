package apoc.util;

import apoc.Description;
import apoc.result.StringResult;
import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 26.05.16
 */
public class Utils {
    @Context
    public GraphDatabaseService db;

    @Context
    public KernelTransaction transaction;

    @Procedure
    @Description("apoc.util.sha1([values]) | computes the sha1 of the concatenation of all string values of the list")
    public Stream<StringResult> sha1(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return Stream.of(new StringResult(DigestUtils.sha1Hex(value)));
    }

    @Procedure
    @Description("apoc.util.md5([values]) | computes the md5 of the concatenation of all string values of the list")
    public Stream<StringResult> md5(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return Stream.of(new StringResult(DigestUtils.md5Hex(value)));
    }

    @Procedure
    @Description("apoc.util.sleep(<duration>) | sleeps for <duration> millis, transaction termination is honored")
    public void sleep(@Name("duration") long duration) throws InterruptedException {
        long started = System.currentTimeMillis();
        while (System.currentTimeMillis()-started < duration) {
            Thread.sleep(5);
            if (transaction.getReasonIfTerminated()!=null) {
                return;
            }
        }
    }
}
