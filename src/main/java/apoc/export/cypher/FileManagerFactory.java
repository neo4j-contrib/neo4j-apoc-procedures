package apoc.export.cypher;

import apoc.util.FileUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mh
 * @since 06.12.17
 */
public class FileManagerFactory {
    public static ExportCypherFileManager createFileManager(String fileName, boolean separatedFiles, boolean b) {
        return fileName == null ?
                new StringExportCypherFileManager(fileName, separatedFiles) :
                new PhysicalExportCypherFileManager(fileName, separatedFiles);
    }

    interface ExportCypherFileManager {
        PrintWriter getPrintWriter(String type) throws IOException;

        String drain(String type);
    }

    private static class PhysicalExportCypherFileManager implements ExportCypherFileManager {

        private final String fileName;
        private boolean separatedFiles;
        private PrintWriter writer;

        public PhysicalExportCypherFileManager(String fileName, boolean separatedFiles) {
            this.fileName = fileName;
            this.separatedFiles = separatedFiles;
        }

        public PrintWriter getPrintWriter(String type) throws IOException {

            if (this.separatedFiles) {
                return FileUtils.getPrintWriter(normalizeFileName(fileName, type), null);
            } else {
                if (this.writer == null) {
                    this.writer = FileUtils.getPrintWriter(normalizeFileName(fileName, null), null);
                }
                return this.writer;
            }
        }

        private String normalizeFileName(final String fileName, String suffix) {
            // TODO check if this should be follow the same rules of FileUtils.readerFor
            return fileName.replace(".cypher", suffix != null ? "." + suffix + ".cypher" : ".cypher");
        }

        @Override
        public String drain(String type) {
            return null;
        }
    }

    private static class StringExportCypherFileManager implements ExportCypherFileManager {

        private final String fileName;
        private boolean separatedFiles;
        private ConcurrentMap<String, StringWriter> writers = new ConcurrentHashMap<>();

        public StringExportCypherFileManager(String fileName, boolean separatedFiles) {
            this.fileName = fileName;
            this.separatedFiles = separatedFiles;
        }

        public PrintWriter getPrintWriter(String type) throws IOException {
            if (this.separatedFiles) {
                return new PrintWriter(writers.compute(type, (key, writer) -> writer == null ? new StringWriter() : writer));
            } else {
                return new PrintWriter(writers.compute("cypher", (key, writer) -> writer == null ? new StringWriter() : writer));
            }
        }

        @Override
        public synchronized String drain(String type) {
            StringWriter writer = writers.get(type);
            if (writer != null) {
                String text = writer.toString();
                writer.getBuffer().setLength(0);
                return text;
            }
            else return null;
        }
    }
}
