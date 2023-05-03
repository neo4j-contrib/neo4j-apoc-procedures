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
package apoc.dv;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.result.NodeResult;
import apoc.result.PathResult;
import apoc.result.VirtualPath;
import apoc.result.VirtualRelationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

@Extended
public class DataVirtualizationCatalog {

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    @Procedure(name = "apoc.dv.catalog.add", mode = Mode.WRITE)
    @Description("Add a virtualized resource configuration")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> add(
            @Name("name") String name,
            @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        return Stream.of(new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log).add(VirtualizedResource.from(name, config)))
                .map(VirtualizedResource::toDTO);
    }

    @Procedure(name = "apoc.dv.catalog.remove", mode = Mode.WRITE)
    @Description("Remove a virtualized resource config by name")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> remove(@Name("name") String name) {
        return new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log)
                .remove(name)
                .map(VirtualizedResource::toDTO);
    }

    @Procedure(name = "apoc.dv.catalog.list", mode = Mode.READ)
    @Description("List all virtualized resource configuration")
    public Stream<VirtualizedResource.VirtualizedResourceDTO> list() {
        return new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log).list()
                .map(VirtualizedResource::toDTO);
    }

    @Procedure(name = "apoc.dv.query", mode = Mode.READ)
    @Description("Query a virtualized resource by name and return virtual nodes")
    public Stream<NodeResult> query(@Name("name") String name,
                                    @Name(value = "params", defaultValue = "{}") Object params,
                                    @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        VirtualizedResource vr = new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), log).get(name);
        final Pair<String, Map<String, Object>> procedureCallWithParams = vr.getProcedureCallWithParams(params, config);
        return tx.execute(procedureCallWithParams.first(), procedureCallWithParams.other())
                .stream()
                .map(m -> (Node) m.get(("node")))
                .map(NodeResult::new);
    }

    @Procedure(name = "apoc.dv.queryAndLink", mode = Mode.READ)
    @Description("Query a virtualized resource by name and return virtual nodes linked using virtual rels to the node passed as first param")
    public Stream<PathResult> queryAndLink(@Name("node") Node node,
                                           @Name("relName") String relName,
                                           @Name("name") String name,
                                           @Name(value = "params", defaultValue = "{}") Object params,
                                           @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        VirtualizedResource vr = new DataVirtualizationCatalogHandler(db, apocConfig.getSystemDb(), null).get(name);
        final RelationshipType relationshipType = RelationshipType.withName(relName);
        final Pair<String, Map<String, Object>> procedureCallWithParams = vr.getProcedureCallWithParams(params, config);
        return tx.execute(procedureCallWithParams.first(), procedureCallWithParams.other())
                .stream()
                .map(m -> (Node) m.get(("node")))
                .map(n -> new VirtualRelationship(node, n, relationshipType))
                .map(r -> new VirtualPath.Builder(r.getStartNode()).push(r).build())
                .map(PathResult::new);
    }

}
