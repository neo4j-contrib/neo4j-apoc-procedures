package apoc.export.cypher;

import java.io.OutputStream;
import apoc.export.util.ExportConfig;
import apoc.util.CompressionAlgo;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static apoc.util.FileUtils.getOutputStream;

/**
 * @author mh
 * @since 06.12.17
 */
public class FileManagerFactory {
    public static ExportFileManager createFileManager(String fileName, boolean separatedFiles) {
        return createFileManager(fileName, separatedFiles, ExportConfig.EMPTY);
    }
    
    public static ExportFileManager createFileManager(String fileName, boolean separatedFiles, ExportConfig config) {
        if (fileName == null || "".equals(fileName)) {
            return new StringExportCypherFileManager(separatedFiles, config);
        }
        fileName = fileName.trim();

        final CompressionAlgo compressionAlgo = CompressionAlgo.valueOf(config.getCompressionAlgo());

        // In case of export with separated files (with bulkImport: true) in case of compressed file retrieve also the compression extension
        // e.g. from test.one.two.csv to test.one.two.nodes.LABELNAME.csv.gz, test.one.two.relationships.RELTYPE.csv.gz, etc...
        // otherwise it becomes test.one.two.nodes.LABELNAME.csv, etc..
        int indexOfDot = StringUtils.lastOrdinalIndexOf(fileName, ".", compressionAlgo.equals(CompressionAlgo.NONE) ? 1 : 2);
        String fileType = fileName.substring(indexOfDot + 1);
        return new PhysicalExportFileManager(fileType, fileName, separatedFiles, config);
    }

    private static class PhysicalExportFileManager implements ExportFileManager {

        private final String fileName;
        private final String fileType;
        private final boolean separatedFiles;
        private final Map<String, PrintWriter> writerCache;
        private ExportConfig config;

        public PhysicalExportFileManager(String fileType, String fileName, boolean separatedFiles, ExportConfig config) {
            this.fileType = fileType;
            this.fileName = fileName;
            this.separatedFiles = separatedFiles;
            this.config = config;
            this.writerCache = new ConcurrentHashMap<>();
        }

        @Override
        public PrintWriter getPrintWriter(String type) {
            String newFileName = this.separatedFiles ? normalizeFileName(fileName, type) : normalizeFileName(fileName, null);
            return writerCache.computeIfAbsent(newFileName, (key) -> {
                OutputStream outputStream = getOutputStream(newFileName, config);
                return outputStream == null ? null : new PrintWriter(outputStream);
            });
        }


        @Override
        public StringWriter getStringWriter(String type) {
            return null;
        }

        private String normalizeFileName(final String fileName, String suffix) {
            // TODO check if this should be follow the same rules of FileUtils.readerFor
            return fileName.replace("." + fileType, "." + (suffix != null ? suffix + "." + fileType : fileType));
        }

        @Override
        public String drain(String type) {
            return null;
        }

        @Override
        public String getFileName() {
            return this.fileName;
        }

        @Override
        public Boolean separatedFiles() {
            return this.separatedFiles;
        }
    }

    private static class StringExportCypherFileManager implements ExportFileManager {

        private final boolean separatedFiles;
        private final ExportConfig config;
        private final ConcurrentMap<String, StringWriter> writers = new ConcurrentHashMap<>();

        public StringExportCypherFileManager(boolean separatedFiles, ExportConfig config) {
            this.separatedFiles = separatedFiles;
            this.config = config;
        }

        @Override
        public PrintWriter getPrintWriter(String type) {
            if (!this.separatedFiles) {
                switch (type) {
                    case "csv":
                    case "json":
                    case "graphml":
                        break;
                    default:
                        type = "cypher";
                }
            }
            return new PrintWriter(getStringWriter(type));
        }

        @Override
        public StringWriter getStringWriter(String type) {
            return writers.computeIfAbsent(type, (key) -> new StringWriter());
        }

        @Override
        public synchronized Object drain(String type) {
            StringWriter writer = writers.get(type);
            if (writer != null) {
                return Util.getStringOrCompressedData(writer, config);
            }
            else return null;
        }

        @Override
        public String getFileName() {
            return null;
        }

        @Override
        public Boolean separatedFiles() {
            return this.separatedFiles;
        }
    }

}
