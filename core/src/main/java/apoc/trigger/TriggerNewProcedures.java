package apoc.trigger;

import apoc.ApocConfig;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


public class TriggerNewProcedures {
    // public for testing purpose
    public static final String TRIGGER_NOT_ROUTED_ERROR = "The procedure should be routed and executed against the LEADER system database";
    public static final String TRIGGER_BAD_TARGET_ERROR = "Triggers can only be installed on user databases.";

    @Context public GraphDatabaseService db;
    
    @Context public Transaction tx;

    private void checkInSystemLeader() {
        TriggerHandlerNewProcedures.checkEnabled();
        // routing check
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(db, SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(TRIGGER_NOT_ROUTED_ERROR);
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
        Util.validateQuery(ApocConfig.apocConfig().getDatabase(databaseName), statement);

        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        TriggerInfo removed = TriggerHandlerNewProcedures.install(databaseName, name, statement, selector, params);
        final TriggerInfo triggerInfo = new TriggerInfo(name, statement, selector, params, true, false);
        if (removed.query != null) {
            return Stream.of( removed, triggerInfo);
        }
        return Stream.of(triggerInfo);
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.drop(databaseName, name) | eventually removes an existing trigger, returns the trigger's information")
    public Stream<TriggerInfo> drop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();
        final TriggerInfo removed = TriggerHandlerNewProcedures.drop(databaseName, name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(removed);
    }
    
    
    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.dropAll(databaseName) | eventually removes all previously added trigger, returns triggers' information")
    public Stream<TriggerInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystemLeader();
        return TriggerHandlerNewProcedures.dropAll(databaseName)
                .stream().sorted(Comparator.comparing(i -> i.name));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.stop(databaseName, name) | eventually pauses the trigger")
    public Stream<TriggerInfo> stop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();

        return Stream.of(
                TriggerHandlerNewProcedures.updatePaused(databaseName, name, true));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.start(databaseName, name) | eventually unpauses the paused trigger")
    public Stream<TriggerInfo> start(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();
        final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, false);

        return Stream.of(triggerInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.trigger.show(databaseName) | it lists all eventually installed triggers for a database")
    public Stream<TriggerInfo> show(@Name("databaseName") String databaseName) {
        checkInSystemLeader();

        return TriggerHandlerNewProcedures.getTriggerNodesList(databaseName, tx);
    }
}