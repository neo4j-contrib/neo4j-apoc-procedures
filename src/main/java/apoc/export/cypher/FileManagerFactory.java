package apoc.export.cypher;

import apoc.util.FileUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mh
 * @since 06.12.17
 */
public class FileManagerFactory {
    public static ExportFileManager createFileManager(String fileName, boolean separatedFiles) {
        if (fileName == null) {
            return new StringExportCypherFileManager(separatedFiles);
        }

        int indexOfDot = fileName.lastIndexOf(".");
        String fileType = fileName.substring(indexOfDot + 1);
        return new PhysicalExportFileManager(fileType, fileName, separatedFiles);
    }

    private static class PhysicalExportFileManager implements ExportFileManager {

        private final String fileName;
        private final String fileType;
        private boolean separatedFiles;
        private PrintWriter writer;

        public PhysicalExportFileManager(String fileType, String fileName, boolean separatedFiles) {
            this.fileType = fileType;
            this.fileName = fileName;
            this.separatedFiles = separatedFiles;
        }

        @Override
        public PrintWriter getPrintWriter(String type) {

            if (this.separatedFiles) {
                return FileUtils.getPrintWriter(normalizeFileName(fileName, type), null);
            } else {
                if (this.writer == null) {
                    this.writer = FileUtils.getPrintWriter(normalizeFileName(fileName, null), null);
                }
                return this.writer;
            }
        }

        @Override
        public StringWriter getStringWriter(String type) {
            return null;
        }

        private String normalizeFileName(final String fileName, String suffix) {
            // TODO check if this should be follow the same rules of FileUtils.readerFor
            return fileName.replace("." + fileType, "." + (suffix != null ? suffix + "." +fileType : fileType));
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

        private boolean separatedFiles;
        private ConcurrentMap<String, StringWriter> writers = new ConcurrentHashMap<>();

        public StringExportCypherFileManager(boolean separatedFiles) {
            this.separatedFiles = separatedFiles;
        }

        @Override
        public PrintWriter getPrintWriter(String type) {
            if (this.separatedFiles) {
                return new PrintWriter(getStringWriter(type));
            } else {
                switch (type) {
                    case "csv":
                    case "json":
                    case "graphml":
                        break;
                    default:
                        type = "cypher";
                }
                return new PrintWriter(getStringWriter(type));
            }
        }

        @Override
        public StringWriter getStringWriter(String type) {
            return writers.computeIfAbsent(type, (key) -> new StringWriter());
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
