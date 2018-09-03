package apoc.uuid;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

public class Uuid {

    @Context
    public GraphDatabaseService db;

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.add(label, {config}) | it will ad the uuid for the Label")
    public Stream<UuidInfo> add(@Name("label") String label, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        Map<String, Object> removed = UuidHandler.add(label, config);
        if (removed != null) {
            return Stream.of(
                    new UuidInfo(label, false),
                    new UuidInfo(label, true));
        }
        return Stream.of(new UuidInfo(label, true));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.remove(label) | it remove the uuid")
    public Stream<UuidInfo> remove(@Name("label") String label) {
        Map<String, Object> removed = UuidHandler.remove(label);
        if (removed == null) {
            return Stream.of(new UuidInfo(null, false));
        }
        return Stream.of(new UuidInfo(label, false));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.removeAll() | it remove all the uuid")
    public Stream<UuidInfo> removeAll() {
        Map<String, Object> removed = UuidHandler.removeAll();
        if (removed == null) {
            return Stream.of(new UuidInfo(null, false));
        }
        return removed.entrySet().stream().map(this::uuidInfo);
    }

    @Procedure(mode = Mode.READ)
    @Description("list all installed uuid")
    public Stream<UuidInfo> list() {
        return UuidHandler.list().entrySet().stream()
                .map((e) -> new UuidInfo(e.getKey(),true));
    }

    private UuidInfo uuidInfo(Map.Entry<String, Object> e) {
        String label = e.getKey();
        if (e.getValue() instanceof Map) {
            try {
                return new UuidInfo(label, false);
            } catch (Exception ex) {
                return new UuidInfo(null, false);
            }
        }
        return new UuidInfo(null, false);
    }

    public static class UuidInfo {
        public final String label;
        public boolean installed;

        UuidInfo(String label, boolean installed) {
            this.label = label;
            this.installed = installed;
        }
    }

    public static class UuidLifeCycle {
        private final GraphDatabaseAPI db;
        private final Log log;
        private UuidHandler uuidHandler;

        public UuidLifeCycle(GraphDatabaseAPI db, Log log) {
            this.db = db;
            this.log = log;
        }

        public void start() {
            boolean enabled = Util.toBoolean(ApocConfiguration.get("uuid.enabled", null));
            if (!enabled) {
                return;
            }

            uuidHandler = new UuidHandler(db, log);
            db.registerTransactionEventHandler(uuidHandler);
        }

        public void stop() {
            if (uuidHandler == null) return;
            db.unregisterTransactionEventHandler(uuidHandler);
        }
    }
}
