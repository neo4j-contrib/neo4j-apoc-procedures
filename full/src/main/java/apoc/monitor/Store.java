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
import apoc.result.StoreInfoResult;
import org.apache.commons.io.FileUtils;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.util.stream.Stream;

@Extended
public class Store {

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.monitor.store() returns informations about the sizes of the different parts of the neo4j graph store")
    public Stream<StoreInfoResult> store() {

        Database database = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Database.class);
        //This will work only on Record format databases. Has to be updated when any additional format is available
        RecordDatabaseLayout databaseLayout = RecordDatabaseLayout.cast( database.getDatabaseLayout() );
        return Stream.of(new StoreInfoResult(
                getDirectorySize(databaseLayout.getTransactionLogsDirectory().toFile()),
                databaseLayout.propertyStringStore().toFile().length(),
                databaseLayout.propertyArrayStore().toFile().length(),
                databaseLayout.relationshipStore().toFile().length(),
                databaseLayout.propertyStore().toFile().length(),
                getDirectorySize(databaseLayout.databaseDirectory().toFile()), //databaseLayout.storeFiles().stream().mapToLong(File::length).sum(),
                databaseLayout.nodeStore().toFile().length()
        ));
    }

    private long getDirectorySize(File folder) {
        return folder.exists() ? FileUtils.sizeOfDirectory(folder) : 0;
    }


}
