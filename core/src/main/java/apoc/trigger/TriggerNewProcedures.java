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
package apoc.trigger;

import apoc.ApocConfig;
import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;


public class TriggerNewProcedures {
    public static final String NON_SYS_DB_ERROR = "The procedure should be executed against a system database.";
    public static final String TRIGGER_NOT_ROUTED_ERROR = "No write operations are allowed directly on this database. " +
            "Writes must pass through the leader. The role of this server is: FOLLOWER";
    public static final String TRIGGER_BAD_TARGET_ERROR = "Triggers can only be installed on user databases.";
    public static final String TRIGGER_QUERY_TYPES_ERROR = "The trigger statement must contain READ_ONLY, WRITE, or READ_WRITE query.";
    public static final String TRIGGER_MODES_ERROR = "The trigger statement cannot contain procedures that are not in WRITE, READ, or DEFAULT mode.";

    @Context public GraphDatabaseAPI db;
    
    @Context public Transaction tx;

    private void checkInSystemLeader() {
        TriggerHandlerNewProcedures.checkEnabled();
        // routing check
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(db, SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(TRIGGER_NOT_ROUTED_ERROR);
        }
    }

    private void checkInSystem() {
        TriggerHandlerNewProcedures.checkEnabled();

        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(NON_SYS_DB_ERROR);
        }
    }

    private void checkTargetDatabase(String databaseName) {
        if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(TRIGGER_BAD_TARGET_ERROR);
        }
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.install(databaseName, name, statement, selector, config) | eventually adds a trigger for a given database which is invoked when a successful transaction occurs.")
    public Stream<TriggerInfo> install(@Name("databaseName") String databaseName, @Name("name") String name, @Name("statement") String statement, @Name(value = "selector")  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        checkInSystemLeader();
        checkTargetDatabase(databaseName);

        // TODO - to be deleted in 5.x, because in a cluster, not all DBMS host all the databases on them,
        // so we have to assume that the leader of the system database doesn't have access to this user database
        Util.validateQuery(ApocConfig.apocConfig().getDatabase(databaseName), statement,
                TRIGGER_MODES_ERROR,
                Set.of(Mode.WRITE, Mode.READ, Mode.DEFAULT),
                TRIGGER_QUERY_TYPES_ERROR,
                READ_ONLY, WRITE, READ_WRITE);

        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());

        return withTransaction(tx -> {
            TriggerInfo triggerInfo = TriggerHandlerNewProcedures.install(databaseName, name, statement, selector, params, tx);
            return Stream.of(triggerInfo);
        });
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.drop(databaseName, name) | eventually removes an existing trigger, returns the trigger's information")
    public Stream<TriggerInfo> drop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();

        return withTransaction(tx -> {
            final TriggerInfo removed = TriggerHandlerNewProcedures.drop(databaseName, name, tx);
            return Stream.ofNullable(removed);
        });
    }
    
    
    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.dropAll(databaseName) | eventually removes all previously added trigger, returns triggers' information")
    public Stream<TriggerInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystemLeader();

        return withTransaction( tx -> TriggerHandlerNewProcedures.dropAll(databaseName, tx)
                .stream().sorted( Comparator.comparing(i -> i.name) )
        );
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.stop(databaseName, name) | eventually pauses the trigger")
    public Stream<TriggerInfo> stop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();

        return withTransaction(tx -> {
            final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, true, tx);
            return Stream.ofNullable(triggerInfo);
        });
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.start(databaseName, name) | eventually unpauses the paused trigger")
    public Stream<TriggerInfo> start(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();

        return withTransaction(tx -> {
            final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, false, tx);
            return Stream.ofNullable(triggerInfo);
        });
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.trigger.show(databaseName) | it lists all eventually installed triggers for a database")
    public Stream<TriggerInfo> show(@Name("databaseName") String databaseName) {
        checkInSystem();

        return TriggerHandlerNewProcedures.getTriggerNodesList(databaseName, tx);
    }


    public <T> T withTransaction(Function<Transaction, T> action) {
        try (Transaction tx = db.beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }
}