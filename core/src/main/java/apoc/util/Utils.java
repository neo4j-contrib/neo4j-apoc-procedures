package apoc.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.procedure.*;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.util.CompressionConfig.CompressionAlgo;
import static apoc.util.Util.convertFromBytesToList;
import static apoc.util.Util.convertFromListToBytes;

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

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(convertFromListToBytes(data))) {
            return (String) makeCompressionAlgo(new CompressionConfig(config), byteArrayInputStream, false, null);
        }
    }

    @UserFunction
    @Description("apoc.util.compress(string, {config}) | return a compressed byte[] in various format from a string")
    public List<Long> compress(@Name("data") String data, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            return (List<Long>) makeCompressionAlgo(new CompressionConfig(config), byteArrayOutputStream, true, data);
        }
    }

    private Object makeCompressionAlgo(CompressionConfig config, Closeable stream, boolean fromStringToByte, String data) throws Exception {
        Charset charset = config.getCharset();
        CompressionAlgo compressionAlgo = config.getCompressionAlgo();
        switch (compressionAlgo) {
            case GZIP:
                return finalizeCompression(GzipCompressorOutputStream.class, GzipCompressorInputStream.class, fromStringToByte, stream, charset, data);
            case BZIP2:
                return finalizeCompression(BZip2CompressorOutputStream.class, BZip2CompressorInputStream.class, fromStringToByte, stream, charset, data);
            case DEFLATE:
                return finalizeCompression(DeflateCompressorOutputStream.class, DeflateCompressorInputStream.class, fromStringToByte, stream, charset, data);
            case BLOCK_LZ4:
                return finalizeCompression(BlockLZ4CompressorOutputStream.class, BlockLZ4CompressorInputStream.class, fromStringToByte, stream, charset, data);
            case FRAMED_SNAPPY:
                return finalizeCompression(FramedSnappyCompressorOutputStream.class, FramedSnappyCompressorInputStream.class, fromStringToByte, stream, charset, data);
            default:
                throw new IllegalArgumentException("Invalid compression algorithm: " + compressionAlgo);
        }
    }

    private <T, S> Object finalizeCompression(Class<T> clazzOutput, Class<S> clazzInput, boolean fromStringToByte, Closeable stream, Charset charset, String data) throws Exception {
        if (fromStringToByte) {
            Constructor<?> constructor = clazzOutput.getConstructor(OutputStream.class);
            try (OutputStream outputStream = (OutputStream) constructor.newInstance((OutputStream) stream)) {
                outputStream.write(data.getBytes(charset));
            }
            return convertFromBytesToList(((ByteArrayOutputStream) stream).toByteArray());
        } else {
            Constructor<?> constructor = clazzInput.getConstructor(InputStream.class);
            try (InputStream inputStream = (InputStream) constructor.newInstance((InputStream) stream)) {
                return inputReader(inputStream, charset);
            }
        }
    }

    private String inputReader(InputStream inputStream, Charset charset) throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream, charset)) {
            try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                StringBuilder output = new StringBuilder();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    output.append(line);
                }
                return output.toString();
            }
        }
    }
}
