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

public class ExportProgressInfoExtended implements ProgressInfoExtended {
    public static final ExportProgressInfoExtended EMPTY = new ExportProgressInfoExtended(null, null, null);

    @Description("The name of the file to which the data was exported.")
    public final String file;

    @Description("A summary of the exported data.")
    public String source;

    @Description("The format the file is exported in.")
    public final String format;

    @Description("The number of exported nodes.")
    public long nodes;

    @Description("The number of exported relationships.")
    public long relationships;

    @Description("The number of exported properties.")
    public long properties;

    @Description("The duration of the export.")
    public long time;

    @Description("The number of rows returned.")
    public long rows;

    @Description("The size of the batches the export was run in.")
    public long batchSize = -1;

    @Description("The number of batches the export was run in.")
    public long batches;

    @Description("Whether the export ran successfully.")
    public boolean done;

    @Description("The data returned by the export.")
    public Object data;

    public ExportProgressInfoExtended(String file, String source, String format) {
        this.file = file;
        this.source = source;
        this.format = format;
    }

    public ExportProgressInfoExtended(ExportProgressInfoExtended pi) {
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

    public ExportProgressInfoExtended update(long nodes, long relationships, long properties) {
        this.nodes += nodes;
        this.relationships += relationships;
        this.properties += properties;
        return this;
    }

    public ExportProgressInfoExtended updateTime(long start) {
        this.time = System.currentTimeMillis() - start;
        return this;
    }

    public ExportProgressInfoExtended done(long start) {
        this.done = true;
        return updateTime(start);
    }

    public void nextRow() {
        this.rows++;
    }

    public ExportProgressInfoExtended drain(StringWriter writer, ExportConfigExtended config) {
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

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }
}
