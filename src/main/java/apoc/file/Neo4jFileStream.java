package apoc.file;

import apoc.ApocConfiguration;
import apoc.util.FileUtils;
import org.neo4j.procedure.*;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Neo4jFileStream {
    @Context
    public Log log;

    public static class FileEntry implements Comparable<FileEntry> {
        public final long lineNo;
        public final String line;
        public final String path;

        public FileEntry(long lineNumber, String data, String path) {
            this.lineNo = lineNumber;
            this.line = data;
            this.path = path;
        }

        public int compareTo(FileEntry o) {
            return new Long(this.lineNo).compareTo(o.lineNo);
        }
    }

    public boolean canonicalPathInNeo4jHome(File f) throws IOException {
        String canonicalPath = f.getCanonicalPath();
        String neo4jHome = ApocConfiguration.get("unsupported.dbms.directories.neo4j_home", null);

        return canonicalPath.contains(neo4jHome);
    }

    @Procedure(mode=Mode.DBMS)
    @Description( "apoc.file.stream(path, { last: n }) - retrieve system or log file contents, optionally return only the last n lines" )
    public Stream<FileEntry> stream(
            @Name("path") String path,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        String neo4jHome = ApocConfiguration.get("unsupported.dbms.directories.neo4j_home", null);

        // Prepend neo4jHome if it's a relative path, and use the user's path otherwise.
        File f = path.startsWith(File.separator) ? new File(path) : new File(neo4jHome, path);

        try {
            if (!canonicalPathInNeo4jHome(f)) {
                log.warn("Warning: user attempting to access file outside of neo4jHome " + f.getCanonicalPath());
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to resolve basic file canonical path", ioe);
        }

        // Throws error if APOC is not configured to permit local file reads.
        FileUtils.checkReadAllowed(f.toURI().toString());

        try {
            Stream<String> stream = Files.lines(Paths.get(f.toURI()));
            final AtomicLong lineNumber = new AtomicLong(0);
            final String p = f.getCanonicalPath();

            Stream<FileEntry> entries = stream.map(line -> new FileEntry(lineNumber.getAndIncrement(), line, p));

            // Useful for tailing logfiles.
            if(config.containsKey("last")) {
                return entries.sorted(Collections.reverseOrder()).limit(new Double(config.get("last").toString()).longValue());
            }

            return entries;
        } catch(IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
