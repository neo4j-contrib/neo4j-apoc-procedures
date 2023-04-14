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
import apoc.result.KernelInfoResult;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.Version;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Date;
import java.util.stream.Stream;

@Extended
public class Kernel {

    private static final String JMX_OBJECT_NAME = "Kernel";
    private static final String READ_ONLY = "ReadOnly";
    private static final String KERNEL_VERSION = "KernelVersion";
    private static final String STORE_ID = "StoreId";
    private static final String START_TIME = "KernelStartTime";
    private static final String DB_NAME = "DatabaseName";
    private static final String STORE_LOG_VERSION = "StoreLogVersion";
    private static final String STORE_CREATION_DATE = "StoreCreationDate";


    @Context
    public GraphDatabaseService graphDatabaseService;

    @Procedure
    @Description("apoc.monitor.kernel() returns informations about the neo4j kernel")
    public Stream<KernelInfoResult> kernel() {
        GraphDatabaseAPI api = ((GraphDatabaseAPI) graphDatabaseService);
        DependencyResolver resolver = api.getDependencyResolver();
        Database database = resolver.resolveDependency(Database.class);
        DatabaseReadOnlyChecker readOnlyChecker = resolver.resolveDependency( DatabaseReadOnlyChecker.class );

        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        Date startDate = new Date(runtimeBean.getStartTime());

        return Stream.of(new KernelInfoResult(
                readOnlyChecker.isReadOnly(),
                Version.getKernelVersion(),
                database.getStoreId().toString(),
                startDate,
                graphDatabaseService.databaseName(),
                database.getStoreId().getStoreVersion(),
                new Date(database.getStoreId().getCreationTime())
        ));
    }

}
