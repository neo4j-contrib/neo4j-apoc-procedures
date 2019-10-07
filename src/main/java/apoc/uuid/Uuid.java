package apoc.uuid;

import apoc.Description;
import apoc.Pools;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
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

    @Context
    public Pools pools;

    @Context
    public UuidHandler uuidHandler;

    @Context
    public Transaction tx;

    @Procedure(mode = Mode.DBMS)
    @Description("CALL apoc.uuid.install(label, {addToExistingNodes: true/false, uuidProperty: 'uuid'}) yield label, installed, properties, batchComputationResult | it will add the uuid transaction handler\n" +
            "for the provided `label` and `uuidProperty`, in case the UUID handler is already present it will be replaced by the new one")
    public Stream<UuidInfo> install(@Name("label") String label, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        UuidConfig uuidConfig = new UuidConfig(config);
        uuidHandler.checkConstraintUuid(tx, label, uuidConfig.getUuidProperty());

        Map<String, Object> addToExistingNodesResult = Collections.emptyMap();
        if (uuidConfig.isAddToExistingNodes()) {
            addToExistingNodesResult = Util.inTx(db, pools, txInThread ->
                    txInThread.execute("CALL apoc.periodic.iterate(" +
                            "\"MATCH (n:" + Util.sanitizeAndQuote(label) + ") RETURN n\",\n" +
                            "\"SET n." + Util.sanitizeAndQuote(uuidConfig.getUuidProperty()) + " = apoc.create.uuid()\", {batchSize:10000, parallel:true})")
                            .next()
            );
        }
        uuidHandler.add(tx, label, uuidConfig.getUuidProperty());
        return Stream.of(new UuidInstallInfo(label, true, Collections.singletonMap("uuidProperty", uuidConfig.getUuidProperty()), addToExistingNodesResult));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.remove(label) yield label, installed, properties | remove previously added uuid handler and returns uuid information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> remove(@Name("label") String label) {
        String removed = uuidHandler.remove(label);
        if (removed == null) {
            return Stream.of(new UuidInfo(null, false));
        }
        return Stream.of(new UuidInfo(label, false, Collections.singletonMap("uuidProperty", removed)));
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.removeAll() yield label, installed, properties | it removes all previously added uuid handlers and returns uuids information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> removeAll() {
        Map<String, String> removed = uuidHandler.removeAll();
        if (removed == null) {
            return Stream.of(new UuidInfo(null, false));
        }
        return removed.entrySet().stream().map(e -> new UuidInfo(e.getKey(), false, Collections.singletonMap("uuidProperty", e.getValue())));
    }

    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.uuid.list() yield label, installed, properties | provides a list of all the uuid handlers installed with the related configuration")
    public Stream<UuidInfo> list() {
        return uuidHandler.list().entrySet().stream()
                .map((e) -> new UuidInfo(e.getKey(),true, Collections.singletonMap("uuidProperty", e.getValue())));
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


    public static class UuidInstallInfo extends UuidInfo {
        public Map<String, Object> batchComputationResult;

        UuidInstallInfo(String label, boolean installed, Map<String, Object> properties, Map<String, Object> batchComputationResult) {
            super(label, installed, properties);
            this.batchComputationResult = batchComputationResult;
        }

    }

}
