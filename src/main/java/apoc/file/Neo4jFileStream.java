package apoc.file;

import apoc.ApocConfiguration;
import apoc.util.FileUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Neo4jFileStream {
    public class FileEntry {
        public long lineNo;
        public String line;
        public String path;

        public FileEntry(long lineNumber, String data, String path) {
            this.lineNo = lineNumber;
            this.line = data;
            this.path = path;
        }
    }

    @Procedure
    @Description( "apoc.file.stream(path) - retrieve system or log file contents" )
    public Stream<FileEntry> stream(@Name("path") String path) {
        String neo4jHome = ApocConfiguration.allConfig.get("unsupported.dbms.directories.neo4j_home");
        // String logDir = ApocConfiguration.allConfig.get("dbms.directories.logs");

        // For security reasons, this procedure should only work with paths relative to neo4j's home.
        // Users may not for example use this to read /etc/password on the system.
        if (path.contains("." + File.separator)) {
            throw new RuntimeException("This procedure does not allow the use of relative paths");
        }

        String fullPath = neo4jHome + File.separator + path;
        String canonicalPath = null;
        File f = new File(fullPath);

        try {
            canonicalPath = f.getCanonicalPath();
            // Open question on whether this should be permitted or not.  It is permitted by other APOC utilities,
            // so for now it's permitted here with a warning.
            if (!canonicalPath.equals(f.getAbsolutePath()) && !canonicalPath.contains(neo4jHome)) {
                System.err.println("Warning: user attempting to access file outside of neo4jHome " +
                        canonicalPath);
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to resolve basic file canonical path", ioe);
        }

        // Throws error if APOC is not configured to permit local file reads.
        FileUtils.checkReadAllowed(f.toURI().toString());

        try {
            Stream<String> stream = Files.lines(Paths.get(fullPath));
            final AtomicLong lineNumber = new AtomicLong(0);
            final String p = canonicalPath;

            return stream.map(line -> new FileEntry(lineNumber.getAndIncrement(), line, p));
        } catch(FileNotFoundException exc) {
            throw new RuntimeException(exc);
        } catch(IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
