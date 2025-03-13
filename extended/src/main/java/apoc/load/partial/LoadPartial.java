package apoc.load.partial;

import apoc.Extended;
import apoc.result.StringResult;
import apoc.util.*;
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
import java.util.Map;
import java.util.stream.Stream;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static apoc.ApocConfig.apocConfig;
import static apoc.load.partial.LoadPartialAws.getS3ObjectInputStream;


@Extended
public class LoadPartial {
    @Context
    public GraphDatabaseService db;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure("apoc.load.stringPartial")
    @Description("apoc.load.stringPartial")
    public Stream<StringResult> offset(@Name("urlOrBinary") Object urlOrBinary,
                                    @Name("offset") long offset,
                                   @Name(value = "limit", defaultValue = "0") long limit,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        String value = getStringResultStream(urlOrBinary, Math.toIntExact(offset), Math.toIntExact(limit), config);
        return Stream.of(new StringResult(value));
    }

    private String getStringResultStream(Object urlOrBinary, int offset, int limit, Map<String, Object> config) throws Exception {
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
            return readFromByteArray(bytes, offset, limit, conf);
        }
        
        throw new RuntimeException("The first parameter must be a String URL or a byte[]");
    }

    public String readFromFile(String path, int offset, int limit, LoadPartialConfig conf) throws Exception {
        SupportedProtocols protocol = FileUtils.from(path);
        if (!protocol.equals(SupportedProtocols.file)) {
            return getPartialString(path, offset, limit, protocol, conf);
        }

        return readFromLocalFile(path, offset, limit, conf);
    }

    private String getPartialString(String path, int offset, int limit, SupportedProtocols protocol, LoadPartialConfig conf) throws Exception {
        boolean s3Protocol = protocol.equals(SupportedProtocols.s3);

        Map<String, Object> headers = new HashMap<>( conf.getHeaders() );
        String httpLimit = limit == 0
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

    private InputStream getInputStream(String path, int offset, int limit, boolean s3Protocol, LoadPartialConfig conf, Map<String, Object> headers) throws IOException, URISyntaxException, URLAccessValidationError {

        InputStream inputStream;
        if (s3Protocol) {
            return getS3ObjectInputStream(path, offset, limit);
        }

        StreamConnection streamConnection = Util.getStreamConnection(path, headers, null, urlAccessChecker);
        inputStream = streamConnection.getInputStream();
        return inputStream;
    }

    private String readFromLocalFile(String filePath, int offset, int limit, LoadPartialConfig conf) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(offset);

            StringBuilder result = new StringBuilder();
            
            byte[] buffer = new byte[conf.getBufferLimit()];
            int bytesRead;
            int totalRead = 0;

            while ((bytesRead = raf.read(buffer)) != -1) { // Read until EOF

                if (limit != 0L && totalRead + bytesRead > limit) {
                    bytesRead = limit - totalRead;
                }
                result.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8)); // Append to StringBuilder
                totalRead += bytesRead;

                if (limit != 0L && totalRead >= limit) {
                    break;
                }
            }
            
            return result.toString();
        }
    }

    public String readFromArchive(ArchiveType type, String path, String csvFileName, int offset, int limit, LoadPartialConfig conf) throws IOException, URISyntaxException, URLAccessValidationError {

        SupportedProtocols from = FileUtils.from(path);
        boolean s3Protocol = from.equals(SupportedProtocols.s3);

        Map<String, Object> headers = new HashMap<>();
        headers.putIfAbsent("Range", "bytes=0-" + conf.getArchiveLimit());

        try (InputStream inputStream = getInputStream(path, offset, limit, s3Protocol, conf, headers);
             ArchiveInputStream is = type.getInputStream(inputStream)) {

            return getPartialString(csvFileName, offset, limit, is, conf);
        }
    }

    private String getPartialString(String fileName, int offset, int limit, ArchiveInputStream is, LoadPartialConfig conf) throws IOException {
        ArchiveEntry archiveEntry;
        while ((archiveEntry = is.getNextEntry()) != null) {
            if (!archiveEntry.isDirectory() && archiveEntry.getName().equals(fileName)) {
                is.skip(offset);

                return getPartialString(limit, is, conf);
            }
        }

        throw new FileNotFoundException("File not found in archive: " + fileName);
    }

    private String readFromByteArray(byte[] data, int offset, int limit, LoadPartialConfig config) throws Exception {
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

    private String getPartialString(int limit, InputStream inputStream, LoadPartialConfig config) throws IOException {
        StringBuilder result = new StringBuilder();
        int totalRead = 0;
        
        byte[] buffer = new byte[limit == 0 ? config.getBufferLimit() : limit];

        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) { // Read until EOF
            if (limit != 0L && totalRead + bytesRead > limit) {
                bytesRead = limit - totalRead;
            }
            result.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8)); // Append to StringBuilder
            totalRead += bytesRead;
            
            if (limit != 0L && totalRead >= limit) {
                break;
            }
        }
        
        return result.toString();
    }
    
}
