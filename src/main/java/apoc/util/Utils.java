package apoc.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author mh
 * @since 26.05.16
 */
public class Utils {
    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

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
    @Description("apoc.util.fail(message, [parameters]) | throw an exception and abort the transaction")
    public void fail(@Name("message") String message, @Name(value = "parameters", defaultValue = "[]") List<Object> parameters) throws RuntimeException {
        String exceptionMessage = String.format(message, parameters.toArray());
        throw new RuntimeException(exceptionMessage);
    }

    @Procedure(value = "apoc.util.assert")
    @Description("apoc.util.assert(predicate, message, [parameters]) | throw an exception and abort the transaction if the predicate is false")
    public void assertPredicate(@Name("predicate") boolean predicate, @Name("message") String message, @Name(value = "parameters", defaultValue = "[]") List<Object> parameters) {
        if (!predicate) {
            String exceptionMessage = String.format(message, parameters.toArray());
            throw new RuntimeException(exceptionMessage);
        }
    }

    @Procedure
    @Description("apoc.util.sleep(<duration>) | sleeps for <duration> millis, transaction termination is honored")
    public void sleep(@Name("duration") long duration) throws InterruptedException {
        long started = System.currentTimeMillis();
        while (System.currentTimeMillis()-started < duration) {
            try {
                Thread.sleep(5);
                terminationGuard.check();
            } catch (TransactionTerminatedException e) {
                return;
            }
        }
    }

    @Procedure
    @Description("apoc.util.validate(predicate, message, params) | if the predicate yields to true raise an exception")
    public void validate(@Name("predicate") boolean predicate, @Name("message") String message, @Name("params") List<Object> params) {
        if (predicate) {
            if (params!=null && !params.isEmpty()) message = String.format(message,params.toArray(new Object[params.size()]));
            throw new RuntimeException(message);
        }
    }
}
