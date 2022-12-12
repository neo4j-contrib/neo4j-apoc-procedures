package apoc.trigger;

import apoc.util.Util;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.trigger.TriggerNewProcedures.TriggerInfo;
/**
 * @author mh
 * @since 20.09.16
 */

public class Trigger {
    // public for testing purpose
    public static final String SYS_NON_LEADER_ERROR = "It's not possible to write into a cluster member with a non-LEADER system database.\n";
    
    @Context
    public GraphDatabaseService db;

    @Context public TriggerHandler triggerHandler;

    @Context public Log log;

    private void preprocessDeprecatedProcedures() {
        final String msgDeprecation = "Please note that the current procedure is deprecated, \n" +
                "it's recommended to use the `apoc.trigger.install`, `apoc.trigger.drop`, `apoc.trigger.dropAll`, `apoc.trigger.stop`, and `apoc.trigger.start` procedures \n" +
                "instead of, respectively, `apoc.trigger.add`, `apoc.trigger.remove`, `apoc.trigger.removeAll`, `apoc.trigger.pause`, and `apoc.trigger.resume`.";

        log.warn(msgDeprecation);

        if (!Util.isWriteableInstance(db, GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(SYS_NON_LEADER_ERROR + msgDeprecation);
        }
    }

    @Admin
    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.install")
    @Description("add a trigger kernelTransaction under a name, in the kernelTransaction you can use {createdNodes}, {deletedNodes} etc., the selector is {phase:'before/after/rollback/afterAsync'} returns previous and new trigger information. Takes in an optional configuration.")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("kernelTransaction") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        preprocessDeprecatedProcedures();
        
        Util.validateQuery(db, statement);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = triggerHandler.add(name, statement, selector, params);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false),
                    new TriggerInfo(name,statement,selector, params,true, false));
        }
        return Stream.of(new TriggerInfo(name,statement,selector, params,true, false));
    }

    @Admin
    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.drop")
    @Description("remove previously added trigger, returns trigger information")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> removed = triggerHandler.remove(name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }

    @Admin
    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.dropAll")
    @Description("removes all previously added trigger, returns trigger information")
    public Stream<TriggerInfo> removeAll() {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> removed = triggerHandler.removeAll();
        if (removed == null) {
            return Stream.of(new TriggerInfo(null, null, null, false, false));
        }
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

    @Admin
    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.trigger.list() | list all currently working triggers for all databases for the session database")
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

    @Admin
    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.stop")
    @Description("CALL apoc.trigger.pause(name) | it pauses the trigger")
    public Stream<TriggerInfo> pause(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> paused = triggerHandler.updatePaused(name, true);

        return Stream.of(new TriggerInfo(name,
                (String)paused.get("statement"),
                (Map<String,Object>) paused.get("selector"),
                (Map<String,Object>) paused.get("params"),true, true));
    }

    @Admin
    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.trigger.start")
    @Description("CALL apoc.trigger.resume(name) | it resumes the paused trigger")
    public Stream<TriggerInfo> resume(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> resume = triggerHandler.updatePaused(name, false);

        return Stream.of(new TriggerInfo(name,
                (String)resume.get("statement"),
                (Map<String,Object>) resume.get("selector"),
                (Map<String,Object>) resume.get("params"),true, false));
    }
}
