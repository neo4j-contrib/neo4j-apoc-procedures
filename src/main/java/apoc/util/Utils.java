package apoc.util;

import apoc.result.StringResult;
import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.*;

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

    @UserFunction
    @Description("apoc.util.sha1([values]) | computes the sha1 of the concatenation of all string values of the list")
    public String sha1(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha1Hex(value);
    }

    @UserFunction
    @Description("apoc.util.md5([values]) | computes the md5 of the concatenation of all string values of the list")
    public String md5(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.md5Hex(value);
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
