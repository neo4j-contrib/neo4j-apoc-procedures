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
package apoc;

import apoc.cypher.CypherInitializer;
import apoc.trigger.TriggerHandler;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@ServiceProvider
public class CoreApocGlobalComponents implements ApocGlobalComponents {

    @Override
    public Map<String,Lifecycle> getServices(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Collections.singletonMap("trigger", new TriggerHandler(db,
                dependencies.databaseManagementService(),
                dependencies.apocConfig(),
                dependencies.log().getUserLog(TriggerHandler.class),
                dependencies.pools(),
                dependencies.scheduler())
        );
    }

    @Override
    public Collection<Class> getContextClasses() {
        return Collections.singleton(TriggerHandler.class);
    }

    @Override
    public Iterable<AvailabilityListener> getListeners(GraphDatabaseAPI db, ApocExtensionFactory.Dependencies dependencies) {
        return Collections.singleton(new CypherInitializer(db,
                dependencies.log().getUserLog(CypherInitializer.class),
                dependencies.databaseManagementService(),
                dependencies.databaseEventListeners())
        );
    }
}
