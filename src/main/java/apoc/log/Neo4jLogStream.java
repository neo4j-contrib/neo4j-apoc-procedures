package apoc.log;

import apoc.util.FileUtils;
import org.neo4j.procedure.*;
import org.neo4j.logging.Log;

import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * @author moxious
 * @since 27.02.19
 */
public class Neo4jLogStream {
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
            return Long.compare(this.lineNo,o.lineNo);
        }
    }

    @Procedure(mode=Mode.DBMS)
    @Description( "apoc.log.stream('neo4j.log', { last: n }) - retrieve log file contents, optionally return only the last n lines" )
    public Stream<FileEntry> stream(
            @Name("path") String logName,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        File logDir = FileUtils.getLogDirectory();

        if (logDir == null) {
            throw new RuntimeException("Neo4j configured dbms.directories.logs points to a directory that " +
                    "does not exist or is not readable.  Please ensure this configuration is correct.");
        }

        // Prepend neo4jHome if it's a relative path, and use the user's path otherwise.
        File f = new File(logDir, logName);

        try {
            String canonicalPath = f.getCanonicalPath();
            if (!canonicalPath.startsWith(logDir.getAbsolutePath())) {
                throw new RuntimeException("The path you are trying to access has a canonical path outside of the logs " +
                        "directory, and this procedure is only permitted to access files in the log directory.  This may " +
                        "occur if the path in question is a symlink or other link.");
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to resolve basic log file canonical path", ioe);
        }

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
        } catch(NoSuchFileException nsf) {
            // This special case we want to throw a custom message and not let this error propagate, because the
            // trace exposes the full path we were checking.
            throw new RuntimeException("No log file exists by that name");
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
