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
package apoc.export.xls;

import static apoc.export.xls.ExportXlsHandler.XLS_MISSING_DEPS_ERROR;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.ProgressInfo;
import apoc.util.MissingDependencyException;
import apoc.util.Util;
import java.util.*;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.poi.ss.usermodel.*;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Extended
public class ExportXls {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    @Procedure
    @Description("apoc.export.xls.all(file,config) - exports whole database as xls to the provided file")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name("config") Map<String, Object> config)
            throws Exception {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportXls(fileName, source, new DatabaseSubGraph(tx), config);
    }

    @Procedure
    @Description(
            "apoc.export.xls.data(nodes,rels,file,config) - exports given nodes and relationships as xls to the provided file")
    public Stream<ProgressInfo> data(
            @Name("nodes") List<Node> nodes,
            @Name("rels") List<Relationship> rels,
            @Name("file") String fileName,
            @Name("config") Map<String, Object> config)
            throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportXls(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description("apoc.export.xls.graph(graph,file,config) - exports given graph object as xls to the provided file")
    public Stream<ProgressInfo> graph(
            @Name("graph") Map<String, Object> graph,
            @Name("file") String fileName,
            @Name("config") Map<String, Object> config)
            throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportXls(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), config);
    }

    @Procedure
    @Description(
            "apoc.export.xls.query(query,file,{config,...,params:{params}}) - exports results from the cypher statement as xls to the provided file")
    public Stream<ProgressInfo> query(
            @Name("query") String query, @Name("file") String fileName, @Name("config") Map<String, Object> config)
            throws Exception {
        Map<String, Object> params = config == null
                ? Collections.emptyMap()
                : (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query, params);
        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportXls(fileName, source, result, config);
    }

    private Stream<ProgressInfo> exportXls(
            @Name("file") String fileName, String source, Object data, Map<String, Object> configMap) throws Exception {
        try {
            return ExportXlsHandler.getProgressInfoStream(fileName, source, data, configMap, apocConfig, db);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(XLS_MISSING_DEPS_ERROR);
        }
    }
}
