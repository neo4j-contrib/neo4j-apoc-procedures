package apoc.trigger;

import apoc.ApocConfig;
import apoc.util.Util;
import org.neo4j.configuration.GraphDatabaseSettings;
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

/**
 * @author mh
 * @since 20.09.16
 */

public class Trigger {

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

    @Context public TriggerHandler triggerHandler;
    
    @Context public Log log;

    private void preprocessDeprecatedProcedures() {
        final String msgDeprecation =
                "use the `apoc.trigger.install`, `apoc.trigger.drop`, `apoc.trigger.dropAll`, `apoc.trigger.stop`, and `apoc.trigger.start` procedures \n" + 
                "instead of, respectively, `apoc.trigger.add`, `apoc.trigger.remove`, `apoc.trigger.removeAll`, `apoc.trigger.pause`, and `apoc.trigger.resume`.";
                
        log.warn("Please note that the current procedure is deprecated, \n" + msgDeprecation);
        
        if (!Util.isWriteableInstance(db, GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException("It's not possible to write into a cluster member with a non-LEADER system database.\n" +
                    "Either the procedure using the bolt against a core protocol with LEADER system database, \n" +
                    "or " + msgDeprecation);
        }
    }

    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.install")
    @Description("add a trigger kernelTransaction under a name, in the kernelTransaction you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback/afterAsync'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("kernelTransaction") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        preprocessDeprecatedProcedures();
        
        Util.validateQuery(db, statement);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = TriggerUtils.add(db.databaseName(), name, statement, selector, params);
        // always add transaction listener
        triggerHandler.reconcileKernelRegistration(true);
        if (!removed.isEmpty()) {
            return Stream.of(
                    new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false),
                    new TriggerInfo(name,statement,selector, params,true, false));
        }
        return Stream.of(new TriggerInfo(name,statement,selector, params,true, false));
    }

    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.drop")
    @Description("remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        Map<String, Object> removed = TriggerUtils.remove(db.databaseName(), name);
        triggerHandler.updateCache();
        if (removed.isEmpty()) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }

    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.dropAll")
    @Description("removes all previously added trigger, returns trigger information")
    public Stream<TriggerInfo> removeAll() {
        preprocessDeprecatedProcedures();
        Map<String, Object> removed = TriggerUtils.removeAll(db.databaseName());
        // always remove transaction listener
        triggerHandler.reconcileKernelRegistration(false);
        return removed.entrySet().stream().map(this::toTriggerInfo);
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

    @Procedure(mode = Mode.READ)
    @Description("list all installed triggers")
    public Stream<TriggerInfo> list() {
        return triggerHandler.list().entrySet().stream()
                .map( (e) -> new TriggerInfo(e.getKey(),
                        (String)e.getValue().get("statement"),
                        (Map<String,Object>) e.getValue().get("selector"),
                        (Map<String, Object>) e.getValue().get("params"),
                        true,
                        (Boolean) e.getValue().getOrDefault("paused", false))
                );
    }
    
    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.stop")
    @Description("CALL apoc.trigger.pause(name) | it pauses the trigger")
    public Stream<TriggerInfo> pause(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        Map<String, Object> paused = TriggerUtils.updatePaused(db.databaseName(), name, true);

        return Stream.of(new TriggerInfo(name,
                (String)paused.get("statement"),
                (Map<String,Object>) paused.get("selector"),
                (Map<String,Object>) paused.get("params"),true, true));
    }

    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.start")
    @Description("CALL apoc.trigger.resume(name) | it resumes the paused trigger")
    public Stream<TriggerInfo> resume(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        Map<String, Object> resume = TriggerUtils.updatePaused(db.databaseName(), name, false);

        return Stream.of(new TriggerInfo(name,
                (String)resume.get("statement"),
                (Map<String,Object>) resume.get("selector"),
                (Map<String,Object>) resume.get("params"),true, false));
    }

    // TODO - change with SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.install(databaseName, name, statement, selector, config) | add a trigger kernelTransaction under a name, in the kernelTransaction you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback/afterAsync'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<TriggerInfo> install(@Name("databaseName") String databaseName, @Name("name") String name, @Name("kernelTransaction") String statement, @Name(value = "selector")  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        Util.validateQuery(ApocConfig.apocConfig().getDatabase(databaseName), statement);

        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = TriggerUtils.add(databaseName, name, statement, selector, params);
        // always add transaction listener
        triggerHandler.reconcileKernelRegistration(true);
        if (!removed.isEmpty()) {
            return Stream.of(
                    new TriggerInfo( name, (String)removed.get( "statement"), (Map<String, Object>) removed.get( "selector"), (Map<String, Object>) removed.get( "params"), false, false),
                    new TriggerInfo( name, statement, selector, params, true, false));
        }
        return Stream.of(new TriggerInfo( name, statement, selector, params, true, false));
    }


    // TODO - change with SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.stop")
    @Description("CALL apoc.trigger.stop(databaseName, name) | remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> drop(@Name("databaseName") String databaseName, @Name("name")String name) {
        Map<String, Object> removed = TriggerUtils.remove(databaseName, name);
        triggerHandler.updateCache();
        if (removed.isEmpty()) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }
    
    
    // TODO - change with SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.dropAll(databaseName) | removes all previously added trigger, returns trigger information")
    public Stream<TriggerInfo> dropAll(@Name("databaseName") String databaseName) {
        Map<String, Object> removed = TriggerUtils.removeAll(databaseName);
        // always remove transaction listener
        triggerHandler.reconcileKernelRegistration(false);
        return removed.entrySet().stream().map(this::toTriggerInfo);
    }

    // TODO - change with SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.stop(databaseName, name) | it pauses the trigger")
    public Stream<TriggerInfo> stop(@Name("databaseName") String databaseName, @Name("name")String name) {
        Map<String, Object> paused = TriggerUtils.updatePaused(databaseName, name, true);

        return Stream.of(new TriggerInfo(name,
                (String)paused.get("statement"),
                (Map<String,Object>) paused.get("selector"),
                (Map<String,Object>) paused.get("params"),true, true));
    }

    // TODO - change with SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.start(databaseName, name) | it resumes the paused trigger")
    public Stream<TriggerInfo> start(@Name("databaseName") String databaseName, @Name("name")String name) {
        Map<String, Object> resume = TriggerUtils.updatePaused(databaseName, name, false);

        return Stream.of(new TriggerInfo(name,
                (String)resume.get("statement"),
                (Map<String,Object>) resume.get("selector"),
                (Map<String,Object>) resume.get("params"),true, false));
    }

}