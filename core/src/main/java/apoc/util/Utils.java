package apoc.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
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
    @Description("apoc.util.sha256([values]) | computes the sha256 of the concatenation of all string values of the list")
    public String sha256(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha256Hex(value);
    }

    @UserFunction
    @Description("apoc.util.sha384([values]) | computes the sha384 of the concatenation of all string values of the list")
    public String sha384(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha384Hex(value);
    }

    @UserFunction
    @Description("apoc.util.sha512([values]) | computes the sha512 of the concatenation of all string values of the list")
    public String sha512(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha512Hex(value);
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

    @UserFunction
    @Description("apoc.util.validatePredicate(predicate, message, params) | if the predicate yields to true raise an exception else returns true, for use inside WHERE subclauses")
    public boolean validatePredicate(@Name("predicate") boolean predicate, @Name("message") String message, @Name("params") List<Object> params) {
        if (predicate) {
            if (params!=null && !params.isEmpty()) message = String.format(message,params.toArray(new Object[params.size()]));
            throw new RuntimeException(message);
        }

        return true;
    }

    @UserFunction
    @Description("apoc.util.decompress(compressed, {config}) | return a string from a compressed byte[] in various format")
    public String decompress(@Name("data") List<Long> data, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        CompressionConfig conf = new CompressionConfig(config);
        return CompressionAlgo.valueOf(conf.getCompressionAlgo()).decompress(data, conf.getCharset());
    }

    @UserFunction
    @Description("apoc.util.compress(string, {config}) | return a compressed byte[] in various format from a string")
    public List<Long> compress(@Name("data") String data, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        CompressionConfig conf = new CompressionConfig(config);
        return CompressionAlgo.valueOf(conf.getCompressionAlgo()).compress(data, conf.getCharset());
    }
}
