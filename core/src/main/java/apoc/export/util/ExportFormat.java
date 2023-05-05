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
package apoc.export.util;

import static java.lang.String.format;

/**
 * @author AgileLARUS
 *
 * @since 06-04-2017
 */
public enum ExportFormat {

    NEO4J_SHELL("neo4j-shell",
            format("COMMIT%n"), format("BEGIN%n"), format("SCHEMA AWAIT%n"), ""),

    CYPHER_SHELL("cypher-shell",
            format(":commit%n"), format(":begin%n"), "", "CALL db.awaitIndexes(%d);%n"),

    PLAIN_FORMAT("plain", "", "", "", ""),

    GEPHI("gephi", "", "", "", ""),

    TINKERPOP("tinkerpop", "", "", "", "");


    private final String format;

    private String commit;

    private String begin;

    private String indexAwait;

    private String schemaAwait;

    ExportFormat(String format, String commit, String begin, String schemaAwait, String indexAwait) {
        this.format = format;
        this.begin = begin;
        this.commit = commit;
        this.schemaAwait = schemaAwait;
        this.indexAwait = indexAwait;
    }

    public static final ExportFormat fromString(String format) {
        if(format != null && !format.isEmpty()){
            for (ExportFormat exportFormat : ExportFormat.values()) {
                if (exportFormat.format.equalsIgnoreCase(format)) {
                    return exportFormat;
                }
            }
        }
        return NEO4J_SHELL;
    }

    public String begin(){
        return this.begin;
    }

    public String commit(){
        return this.commit;
    }

    public String schemaAwait(){
        return this.schemaAwait;
    }

    public String indexAwait(long millis){
        return format(this.indexAwait, millis);
    }
}
