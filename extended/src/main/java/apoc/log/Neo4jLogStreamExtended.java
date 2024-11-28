/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.log;

import apoc.util.FileUtils;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @author moxious
 * @since 27.02.19
 */
public class Neo4jLogStreamExtended {

    public static class FileEntry implements Comparable<FileEntry> {
        @Description("The line number.")
        public final long lineNo;

        @Description("The content of the line.")
        public final String line;

        @Description("The path to the log file.")
        public final String path;

        public FileEntry(long lineNumber, String data, String path) {
            this.lineNo = lineNumber;
            this.line = data;
            this.path = path;
        }

        public int compareTo(FileEntry o) {
            return Long.compare(this.lineNo, o.lineNo);
        }
    }

    @Admin
    @Procedure(
            name = "apoc.log.stream",
            mode = Mode.DBMS)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the file contents from the given log, optionally returning only the last n lines.\n"
            + "This procedure requires users to have an admin role.")
    public Stream<FileEntry> stream(
            @Name(value = "path", description = "The name of the log file to read.") String logName,
            @Name(value = "config", defaultValue = "{}", description = "{ last :: INTEGER }")
                    Map<String, Object> config) {

        File logDir = FileUtils.getLogDirectory();

        if (logDir == null) {
            throw new RuntimeException("Neo4j configured server.directories.logs points to a directory that "
                    + "does not exist or is not readable.  Please ensure this configuration is correct.");
        }

        // Prepend neo4jHome if it's a relative path, and use the user's path otherwise.
        File f = new File(logDir, logName);

        try {
            if (!f.getCanonicalFile().toPath().startsWith(logDir.getAbsolutePath())) {
                throw new RuntimeException("The path you are trying to access has a canonical path outside of the logs "
                        + "directory, and this procedure is only permitted to access files in the log directory.  This may "
                        + "occur if the path in question is a symlink or other link.");
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
            if (config.containsKey("last")) {
                return entries.sorted(Collections.reverseOrder())
                        .limit(Double.valueOf(config.get("last").toString()).longValue());
            }

            return entries;
        } catch (NoSuchFileException nsf) {
            // This special case we want to throw a custom message and not let this error propagate, because the
            // trace exposes the full path we were checking.
            throw new RuntimeException("No log file exists by that name");
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
