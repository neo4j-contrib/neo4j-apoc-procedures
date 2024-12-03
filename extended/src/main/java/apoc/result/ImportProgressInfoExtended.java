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
package apoc.result;

import apoc.export.util.ExportConfigExtended;
import apoc.util.UtilExtended;
import org.neo4j.procedure.Description;

import java.io.StringWriter;

public class ImportProgressInfoExtended implements ProgressInfoExtended {
    public static final ImportProgressInfoExtended EMPTY = new ImportProgressInfoExtended(null, null, null);

    @Description("The name of the file from which the data was imported.")
    public final String file;

    @Description("The source of the imported data: \"file\", \"binary\" or \"file/binary\".")
    public String source;

    @Description("The format of the file: [\"csv\", \"graphml\", \"json\"].")
    public final String format;

    @Description("The number of imported nodes.")
    public long nodes;

    @Description("The number of imported relationships.")
    public long relationships;

    @Description("The number of imported properties.")
    public long properties;

    @Description("The duration of the import.")
    public long time;

    @Description("The number of rows returned.")
    public long rows;

    @Description("The size of the batches the import was run in.")
    public long batchSize = -1;

    @Description("The number of batches the import was run in.")
    public long batches;

    @Description("Whether the import ran successfully.")
    public boolean done;

    @Description("The data returned by the import.")
    public Object data;

    public ImportProgressInfoExtended(String file, String source, String format) {
        this.file = file;
        this.source = source;
        this.format = format;
    }

    public ImportProgressInfoExtended(ImportProgressInfoExtended pi) {
        this.file = pi.file;
        this.source = pi.source;
        this.format = pi.format;
        this.nodes = pi.nodes;
        this.relationships = pi.relationships;
        this.properties = pi.properties;
        this.time = pi.time;
        this.rows = pi.rows;
        this.batchSize = pi.batchSize;
        this.batches = pi.batches;
        this.done = pi.done;
    }

    @Override
    public String toString() {
        return String.format("nodes = %d rels = %d properties = %d", nodes, relationships, properties);
    }

    public ImportProgressInfoExtended update(long nodes, long relationships, long properties) {
        this.nodes += nodes;
        this.relationships += relationships;
        this.properties += properties;
        return this;
    }

    public ImportProgressInfoExtended updateTime(long start) {
        this.time = System.currentTimeMillis() - start;
        return this;
    }

    public ImportProgressInfoExtended done(long start) {
        this.done = true;
        return updateTime(start);
    }

    public void nextRow() {
        this.rows++;
    }

    public ImportProgressInfoExtended drain(StringWriter writer, ExportConfigExtended config) {
        if (writer != null) {
            this.data = UtilExtended.getStringOrCompressedData(writer, config);
        }
        return this;
    }

    @Override
    public void setBatches(long batches) {
        this.batches = batches;
    }

    @Override
    public void setRows(long rows) {
        this.rows = rows;
    }

    @Override
    public long getBatchSize() {
        return this.batchSize;
    }
}
