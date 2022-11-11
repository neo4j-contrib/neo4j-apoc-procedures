package apoc.trigger;

import apoc.ApocConfig;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


public class TriggerNewProcedures {
    // public for testing purpose
    public static final String TRIGGER_NOT_ROUTED_ERROR = "The procedure should be routed and executed against a LEADER system database";

    public static class TriggerInfo {
        public String name;
        public String query;
        public Map<String,Object> selector;
        public Map<String, Object> params;
        public boolean installed;
        public boolean paused;
        public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed, boolean paused) {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.installed = installed;
            this.paused = paused;
        }

        public TriggerInfo( String name, String query, Map<String,Object> selector, Map<String,Object> params, boolean installed, boolean paused )
        {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.params = params;
            this.installed = installed;
            this.paused = paused;
        }
    }

    @Context public GraphDatabaseService db;
    
    @Context public Log log;

    private void checkInSystemLeader() {
        TriggerHandlerWrite.checkEnabled();
        // routing check
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(db, SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(TRIGGER_NOT_ROUTED_ERROR);
        }
    }

    public TriggerInfo toTriggerInfo(Map.Entry<String, Object> e) {
        String name = e.getKey();
        if (e.getValue() instanceof Map) {
            try {
                Map<String, Object> value = (Map<String, Object>) e.getValue();
                return new TriggerInfo(name, (String) value.get("statement"), (Map<String, Object>) value.get("selector"), (Map<String, Object>) value.get("params"), false, false);
            } catch(Exception ex) {
                return new TriggerInfo(name, ex.getMessage(), null, false, false);
            }
        }
        return new TriggerInfo(name, null, null, false, false);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.install(databaseName, name, statement, selector, config) | add a trigger kernelTransaction under a name, in the kernelTransaction you can use $createdNodes, $deletedNodes etc., the selector is {phase:'before/after/rollback/afterAsync'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<TriggerInfo> install(@Name("databaseName") String databaseName, @Name("name") String name, @Name("kernelTransaction") String statement, @Name(value = "selector")  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        checkInSystemLeader();
        // TODO - to be deleted in 5.x, because in a cluster, not all DBMS host all the databases on them,
        // so we have to assume that the leader of the system database doesn't have access to this user database
        Util.validateQuery(ApocConfig.apocConfig().getDatabase(databaseName), statement);

        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = TriggerHandlerWrite.install(databaseName, name, statement, selector, params);
        if (!removed.isEmpty()) {
            return Stream.of(
                    new TriggerInfo( name, (String)removed.get( "statement"), (Map<String, Object>) removed.get( "selector"), (Map<String, Object>) removed.get( "params"), false, false),
                    new TriggerInfo( name, statement, selector, params, true, false));
        }
        return Stream.of(new TriggerInfo( name, statement, selector, params, true, false));
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.drop(databaseName, name) | remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> drop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();
        Map<String, Object> removed = TriggerHandlerWrite.drop(databaseName, name);
        if (removed.isEmpty()) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }
    
    
    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.dropAll(databaseName) | removes all previously added trigger, returns trigger information")
    public Stream<TriggerInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystemLeader();
        Map<String, Object> removed = TriggerHandlerWrite.dropAll(databaseName);
        return removed.entrySet().stream().map(this::toTriggerInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.stop(databaseName, name) | it pauses the trigger")
    public Stream<TriggerInfo> stop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();
        Map<String, Object> paused = TriggerHandlerWrite.updatePaused(databaseName, name, true);

        return Stream.of(new TriggerInfo(name,
                (String)paused.get("statement"),
                (Map<String,Object>) paused.get("selector"),
                (Map<String,Object>) paused.get("params"),true, true));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.start(databaseName, name) | it resumes the paused trigger")
    public Stream<TriggerInfo> start(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemLeader();
        Map<String, Object> resume = TriggerHandlerWrite.updatePaused(databaseName, name, false);

        return Stream.of(new TriggerInfo(name,
                (String)resume.get("statement"),
                (Map<String,Object>) resume.get("selector"),
                (Map<String,Object>) resume.get("params"),true, false));
    }

}