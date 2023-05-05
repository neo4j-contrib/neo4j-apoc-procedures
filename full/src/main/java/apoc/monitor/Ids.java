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
package apoc.monitor;

import apoc.Extended;
import apoc.result.IdsResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

@Extended
public class Ids {

    private static final String JMX_OBJECT_NAME = "Primitive count";
    private static final String NODE_IDS_KEY = "NumberOfNodeIdsInUse";
    private static final String REL_IDS_KEY = "NumberOfRelationshipIdsInUse";
    private static final String PROP_IDS_KEY = "NumberOfPropertyIdsInUse";
    private static final String REL_TYPE_IDS_KEY = "NumberOfRelationshipTypeIdsInUse";

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.monitor.ids() returns the object ids in use for this neo4j instance")
    public Stream<IdsResult> ids() {

        StoreEntityCounters storeEntityCounters = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(StoreEntityCounters.class);
        return Stream.of(new IdsResult(
                storeEntityCounters.nodes(),
                storeEntityCounters.relationships(),
                storeEntityCounters.properties(),
                storeEntityCounters.relationshipTypes()
        ));
    }
}
