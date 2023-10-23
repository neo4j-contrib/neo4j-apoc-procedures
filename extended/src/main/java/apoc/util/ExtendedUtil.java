package apoc.util;

import static apoc.export.cypher.formatter.CypherFormatterUtils.formatProperties;
import static apoc.export.cypher.formatter.CypherFormatterUtils.formatToString;
import static apoc.util.JsonUtil.OBJECT_MAPPER;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import apoc.ml.bedrock.BedrockConfig;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.neo4j.graphdb.Entity;

public class ExtendedUtil
{

    /**
     * Get the {@link HttpRequestBase} from the method name
     * Similar to <a href="https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/http/apache/request/impl/ApacheHttpRequestFactory.java#L118">aws implementation</a>
     */
    public static HttpRequestBase fromMethodName(String method, String uri) {
        return switch (method) {
            case HttpHead.METHOD_NAME -> new HttpHead(uri);
            case HttpGet.METHOD_NAME -> new HttpGet(uri);
            case HttpDelete.METHOD_NAME -> new HttpDelete(uri);
            case HttpOptions.METHOD_NAME -> new HttpOptions(uri);
            case HttpPatch.METHOD_NAME -> new HttpPatch(uri);
            case HttpPost.METHOD_NAME -> new HttpPost(uri);
            case HttpPut.METHOD_NAME -> new HttpPut(uri);
            default -> throw new RuntimeException("Unknown HTTP method name: " + method);
        };
    }

    /**
     * Similar to JsonUtil.loadJson(..) but works e.g. with GET method as well,
     * for which it would return a FileNotFoundException
     */
    public static Stream<Object> getHttpResponse(BedrockConfig conf, String method, HttpClient httpClient, String payloadString, Map<String, Object> headers, String endpoint, String path, List<String> pathOptions) {

        try {
            // -- request with headers and payload
            HttpRequestBase request = fromMethodName(method, endpoint);

            headers.forEach((k, v) -> request.setHeader(k, v.toString()));

            if (request instanceof HttpEntityEnclosingRequestBase entityRequest) {
                entityRequest.setEntity(new StringEntity(payloadString));
            }

            // -- response
            HttpResponse response = httpClient.execute(request);
            InputStream stream = response.getEntity().getContent();
            checkResponseSuccess(response, stream);

            return streamObjetsFromIStream(stream, path, pathOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void checkResponseSuccess(HttpResponse response, InputStream stream) throws IOException {
        if (response.getStatusLine().getStatusCode() / 100 != 2) {
            String responseContent = new String(stream.readAllBytes());
            throw new IOException("The request is failed with the response: " + responseContent);
        }
    }

    /**
     * Along the lines of {@link JsonUtil#loadJson(Object, Map, String, String, boolean, List)} 
     *  after the `FileUtils.inputStreamFor` method
     */
    public static Stream<Object> streamObjetsFromIStream(InputStream input, String path, List<String> options) {
        try {
            JsonParser parser = OBJECT_MAPPER.getFactory().createParser(input);
            MappingIterator<Object> it = OBJECT_MAPPER.readValues(parser, Object.class);
            Stream<Object> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
            return StringUtils.isBlank(path) ? stream : stream.map((value) -> JsonPath.parse(value, Configuration.builder().build()).read(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    
    public static String dateFormat( TemporalAccessor value, String format){
        return Util.getFormat(format).format(value);
    }

    public static double doubleValue( Entity pc, String prop, Number defaultValue) {
        return Util.toDouble(pc.getProperty(prop, defaultValue));
    }

    public static Duration durationParse(String value) {
        return Duration.parse(value);
    }

    public static boolean isSumOutOfRange(long... numbers) {
        try {
            sumLongs(numbers).longValueExact();
            return false;
        } catch (ArithmeticException ae) {
            return true;
        }
    }

    private static BigInteger sumLongs(long... numbers) {
        return LongStream.of(numbers)
                .mapToObj(BigInteger::valueOf)
                .reduce(BigInteger.ZERO, (x, y) -> x.add(y));
    }

    public static String toCypherMap( Map<String, Object> map) {
        final StringBuilder builder = formatProperties(map);
        return "{" + formatToString(builder) + "}";
    }
}
