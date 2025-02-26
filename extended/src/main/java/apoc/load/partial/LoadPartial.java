package apoc.load.partial;

import apoc.Extended;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import apoc.util.*;
import apoc.util.s3.S3Aws;
import apoc.util.s3.S3Params;
import apoc.util.s3.S3ParamsExtractor;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.CompressionConfig.COMPRESSION;


@Extended
public class LoadPartial {
    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.load.jsonPartial")
    @Description("apoc.load.jsonPartial")
    public Stream<ObjectResult> json(@Name("urlOrBinary") Object urlOrBinary,
                                    @Name("offset") long offset,
                                    @Name(value = "limit", defaultValue = "0") long limit,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        String value = getStringResultStream(urlOrBinary, offset, limit, config);

        String jsonPath = (String) config.getOrDefault("jsonPath", "");
        List<String> pathOptions = (List<String>) config.getOrDefault("pathOptions", List.of());
        return Stream.of(
                new ObjectResult( JsonUtil.parse(value, jsonPath, Object.class, pathOptions) )
        );
    }

    @Procedure("apoc.load.stringPartial")
    @Description("apoc.load.stringPartial")
    public Stream<StringResult> offset(@Name("urlOrBinary") Object urlOrBinary,
                                    @Name("offset") long offset,
                                   @Name(value = "limit", defaultValue = "0") long limit,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        String value = getStringResultStream(urlOrBinary, offset, limit, config);
        return Stream.of(new StringResult(value));
    }

    private String getStringResultStream(Object urlOrBinary, long offset, long limit, Map<String, Object> config) throws Exception {
        LoadPartialConfig conf = new LoadPartialConfig(config);
        if (urlOrBinary instanceof String filePath) {
            apocConfig().checkReadAllowed(filePath, urlAccessChecker);
            final ArchiveType archiveType = ArchiveType.from(filePath);
            if (archiveType.isArchive()) {
                String[] tokens = filePath.split("!");
                
                return readFromArchive(archiveType, tokens[0], tokens[1], offset, limit, conf);
            } else {
                return readFromFile(filePath, offset, limit, conf);
            }

        } 
        if (urlOrBinary instanceof byte[] bytes) {
            return readFromByteArray(bytes, (int) offset, limit, conf);
        }
        
        throw new RuntimeException("The first parameter must be a String URL or a byte[]");
    }

    public String readFromFile(String path, Long offset, long limit, LoadPartialConfig conf) throws Exception {
        SupportedProtocols protocol = FileUtils.from(path);
        if (!protocol.equals(SupportedProtocols.file)) {
            return getPartialString(path, offset, limit, protocol, conf);
        }

        return readFromLocalFile(path, offset, limit, conf);
    }

    private String getPartialString(String path, Long offset, long limit, SupportedProtocols protocol, LoadPartialConfig conf) throws Exception {
        boolean s3Protocol = protocol.equals(SupportedProtocols.s3);

        Map<String, Object> headers = new HashMap<>( conf.getHeaders() );
        String httpLimit = limit == 0L
                ? "-"
                : "-" + (offset + limit - 1);
        headers.putIfAbsent("Range", "bytes=" + offset + httpLimit);
        
        try (InputStream inputStream = getInputStream(path, offset, limit, s3Protocol, conf, headers)) {
            
            // in case of http(s), ftp and gs we use the "Range" header to retrieve the portion,
            // otherwise we need to skip the output stream
            if (s3Protocol || protocol.equals(SupportedProtocols.hdfs)) {
                inputStream.skip(offset);
            }

            return getPartialString(limit, inputStream, conf);
        }
    }

    private InputStream getInputStream(String path, Long offset, long limit, boolean s3Protocol, LoadPartialConfig conf, Map<String, Object> headers) throws IOException, URISyntaxException, URLAccessValidationError {
        InputStream inputStream;
        StreamConnection streamConnection = Util.getStreamConnection(path, headers, null, urlAccessChecker);

        if (s3Protocol) {
            S3Params s3Params = S3ParamsExtractor.extract(path);
            String region = Objects.nonNull(s3Params.getRegion()) ? s3Params.getRegion() : Regions.US_EAST_1.getName();
            S3Aws s3Aws = new S3Aws(s3Params, region);

            GetObjectRequest request = new GetObjectRequest(s3Params.getBucket(), s3Params.getKey());
            
            if (limit == 0L) {
                request.withRange(offset);
            } else {
                request.withRange(offset, offset + limit - 1);
            }

            S3Object object = s3Aws.getClient().getObject(request);
            inputStream = object.getObjectContent();
        }

        inputStream = streamConnection.getInputStream();
        return inputStream;
    }

    private String readFromLocalFile(String filePath, Long offset, long limit, LoadPartialConfig conf) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(offset);
            
            byte[] buffer = new byte[conf.getBufferLimit()];
            int bytesRead = limit == 0L
                    ? raf.read(buffer)
                    : raf.read(buffer, 0, Math.toIntExact(limit));
            return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
        }
    }

    public String readFromArchive(ArchiveType type, String path, String csvFileName, long offset, long limit, LoadPartialConfig conf) throws IOException, URISyntaxException, URLAccessValidationError {

        SupportedProtocols from = FileUtils.from(path);
        boolean s3Protocol = from.equals(SupportedProtocols.s3);

        Map<String, Object> headers = new HashMap<>();
        headers.putIfAbsent("Range", "bytes=0-" + conf.getArchiveLimit());

        try (InputStream inputStream = getInputStream(path, offset, limit, s3Protocol, conf, headers);
             ArchiveInputStream is = type.getInputStream(inputStream)) {

            return getPartialString(csvFileName, offset, limit, is, conf);
        }
    }

    private static String getPartialString(String fileName, long offset, long limit, ArchiveInputStream is, LoadPartialConfig conf) throws IOException {
        ArchiveEntry archiveEntry;
        while ((archiveEntry = is.getNextEntry()) != null) {
            if (!archiveEntry.isDirectory() && archiveEntry.getName().equals(fileName)) {
                is.skip(offset);

                return getPartialString(limit, is, conf);
            }
        }

        throw new FileNotFoundException("File not found in archive: " + fileName);
    }

    private static String readFromByteArray(byte[] data, int offset, long limit, LoadPartialConfig config) throws Exception {
        if (offset >= data.length) {
            return "";
        }

        CompressionAlgo algo = CompressionAlgo.valueOf( config.getCompressionAlgo() );
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data);
             InputStream inputStream = algo.getInputStream(stream)) {

            inputStream.skip(offset);
            return getPartialString(limit, inputStream, config);
        }
    }

    private static String getPartialString(long limit, InputStream inputStream, LoadPartialConfig config) throws IOException {
        byte[] buffer = new byte[config.getBufferLimit()];
        int bytesRead = limit == 0L
                ? inputStream.read(buffer)
                : inputStream.read(buffer, 0, (int) limit);
        return (bytesRead > 0) ? new String(buffer, 0, bytesRead, StandardCharsets.UTF_8) : "";
    }
    
}
