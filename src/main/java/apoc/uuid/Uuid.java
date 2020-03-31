package apoc.uuid;

import apoc.ApocConfiguration;
import apoc.Description;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public class Uuid {

    @Context
    public GraphDatabaseService db;

    @Procedure(mode = Mode.DBMS)
    @Description("CALL apoc.uuid.install(label, {addToExistingNodes: true/false, uuidProperty: 'uuid'}) yield label, installed, properties, batchComputationResult | it will add the uuid transaction handler\n" +
            "for the provided `label` and `uuidProperty`, in case the UUID handler is already present it will be replaced by the new one")
    public Stream<UuidInstallInfo> install(@Name("label") String label, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        UuidConfig uuidConfig = new UuidConfig(config);
        UuidHandler.checkConstraintUuid(label, uuidConfig);
        Map<String, Object> addToExistingNodesResult = Collections.emptyMap();
        if (uuidConfig.isAddToExistingNodes()) {
            addToExistingNodesResult = Util.inTx(db, () ->
                    db.execute("CALL apoc.periodic.iterate(" +
                            "\"MATCH (n:" + Util.sanitizeAndQuote(label) + ") RETURN n\",\n" +
                            "\"SET n." + Util.sanitizeAndQuote(uuidConfig.getUuidProperty()) + " = apoc.create.uuid()\", {batchSize:10000, parallel:true})")
                            .next()
            );
        }
        UuidConfig removed = UuidHandler.add(label, uuidConfig);
        config = JsonUtil.OBJECT_MAPPER.convertValue(uuidConfig, Map.class); // return the applied configuration (with defaults if the original config was null or empty)
        if (removed != null) {
            return Stream.of(
                    new UuidInstallInfo(label, true, config, addToExistingNodesResult));
        }
        return Stream.of(new UuidInstallInfo(label, true, config, addToExistingNodesResult));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.remove(label) yield label, installed, properties | remove previously added uuid handler and returns uuid information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> remove(@Name("label") String label) {
        UuidConfig removed = UuidHandler.remove(label);
        if (removed == null) {
            return Stream.of(new UuidInfo(null, false));
        }
        return Stream.of(new UuidInfo(label, false, JsonUtil.OBJECT_MAPPER.convertValue(removed, Map.class)));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.removeAll() yield label, installed, properties | it removes all previously added uuid handlers and returns uuids information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> removeAll() {
        Map<String, Object> removed = UuidHandler.removeAll();
        if (removed == null) {
            return Stream.of(new UuidInfo(null, false));
        }
        return removed.entrySet().stream().map(this::uuidInfo);
    }

    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.uuid.list() yield label, installed, properties | provides a list of all the uuid handlers installed with the related configuration")
    public Stream<UuidInfo> list() {
        return UuidHandler.list().entrySet().stream()
                .map((e) -> new UuidInfo(e.getKey(),true, JsonUtil.OBJECT_MAPPER.convertValue(e.getValue(), Map.class)));
    }

    private UuidInfo uuidInfo(Map.Entry<String, Object> e) {
        String label = e.getKey();
        try {
            if (e.getValue() instanceof Map) {
                return new UuidInfo(label, false, (Map<String, Object>) e.getValue());
            } else if (e.getValue() instanceof UuidConfig) {
                return new UuidInfo(null, false, JsonUtil.OBJECT_MAPPER.convertValue(e.getValue(), Map.class));
            }
        } catch (Exception ex) {
            return new UuidInfo(null, false);
        }
        return new UuidInfo(null, false);
    }

    public static class UuidInfo {
        public final String label;
        public boolean installed;
        public Map<String, Object> properties;

        UuidInfo(String label, boolean installed, Map<String, Object> properties) {
            this.label = label;
            this.installed = installed;
            this.properties = properties;
        }

        UuidInfo(String label, boolean installed) {
            this(label, installed, Collections.emptyMap());
        }
    }


    public static class UuidInstallInfo {
        public Map<String, Object> batchComputationResult;
        public final String label;
        public boolean installed;
        public Map<String, Object> properties;

        UuidInstallInfo(String label, boolean installed, Map<String, Object> properties, Map<String, Object> batchComputationResult) {
            this.label = label;
            this.installed = installed;
            this.properties = properties;
            this.batchComputationResult = batchComputationResult;
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
