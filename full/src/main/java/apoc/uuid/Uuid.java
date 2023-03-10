package apoc.uuid;

import apoc.ApocConfig;
import apoc.Extended;
import apoc.Pools;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.SystemDbUtil.checkWriteAllowed;

@Extended
public class Uuid {
    private static final String MSG_DEPRECATION = "Please note that the current procedure is deprecated, \n" +
            "it's recommended to use the `apoc.uuid.create`, `apoc.uuid.drop`, `apoc.uuid.dropAll` procedures \n" +
            "instead of, respectively, `apoc.uuid.install`, `apoc.uuid.remove`, `apoc.uuid.removeAll`.";

    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public UuidHandler uuidHandler;

    @Context
    public Transaction tx;


    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.uuid.create")
    @Description("CALL apoc.uuid.install(label, {addToExistingNodes: true/false, uuidProperty: 'uuid'}) yield label, installed, properties, batchComputationResult | it will add the uuid transaction handler\n" +
            "for the provided `label` and `uuidProperty`, in case the UUID handler is already present it will be replaced by the new one")
    public Stream<UuidInstallInfo> install(@Name("label") String label, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        checkWriteAllowed(db, MSG_DEPRECATION);

        UuidConfig uuidConfig = new UuidConfig(config);
        uuidHandler.checkConstraintUuid(tx, label, uuidConfig.getUuidProperty());

        Map<String, Object> addToExistingNodesResult = Collections.emptyMap();
        if (uuidConfig.isAddToExistingNodes()) {
            addToExistingNodesResult = setExistingNodes(db, pools, label, uuidConfig);
        }
        uuidHandler.add(tx, label, uuidConfig);
        return Stream.of(new UuidInstallInfo(label, true,
                Map.of("uuidProperty", uuidConfig.getUuidProperty(), "addToSetLabels", uuidConfig.isAddToSetLabels()),
                addToExistingNodesResult));
    }

    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.uuid.drop")
    @Description("CALL apoc.uuid.remove(label) yield label, installed, properties | remove previously added uuid handler and returns uuid information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> remove(@Name("label") String label) {
        checkWriteAllowed(db, MSG_DEPRECATION);

        UuidConfig removed = uuidHandler.remove(label);
        if (removed == null) {
            return Stream.of(new UuidInfo(false));
        }
        return Stream.of(new UuidInfo(label, false, 
                Map.of("uuidProperty", removed.getUuidProperty(), "addToSetLabels", removed.isAddToSetLabels())));
    }

    @Deprecated
    @Procedure(mode = Mode.WRITE, deprecatedBy = "apoc.uuid.dropAll")
    @Description("CALL apoc.uuid.removeAll() yield label, installed, properties | it removes all previously added uuid handlers and returns uuids information. All the existing uuid properties are left as-is")
    public Stream<UuidInfo> removeAll() {
        checkWriteAllowed(db, MSG_DEPRECATION);

        Map<String, UuidConfig> removed = uuidHandler.removeAll();
        if (removed == null) {
            return Stream.of(new UuidInfo(false));
        }
        return removed.entrySet().stream().map(e -> {
            final UuidConfig conf = e.getValue();
            return new UuidInfo(e.getKey(), false, Map.of("uuidProperty", conf.getUuidProperty(), "addToSetLabels", conf.isAddToSetLabels()));
        });
    }

    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.uuid.list() yield label, installed, properties | provides a list of all the uuid handlers installed with the related configuration")
    public Stream<UuidInfo> list() {
        return uuidHandler.list().entrySet().stream()
                .map((e) -> {
                    final UuidConfig conf = e.getValue();
                    return new UuidInfo(e.getKey(),true, Map.of("uuidProperty", conf.getUuidProperty(), "addToSetLabels", conf.isAddToSetLabels()));
                });
    }

    public static class UuidInstallInfo extends UuidInfo {
        public Map<String, Object> batchComputationResult;

        UuidInstallInfo(String label, boolean installed, Map<String, Object> properties, Map<String, Object> batchComputationResult) {
            super(label, installed, properties);
            this.batchComputationResult = batchComputationResult;
        }

    }

    public static Map<String, Object> setExistingNodes(GraphDatabaseService db, Pools pools, String label, UuidConfig uuidConfig) {
        final String uuidFunctionName = getUuidFunctionName();

        return Util.inTx(db, pools, txInThread ->
                txInThread.execute("CALL apoc.periodic.iterate(" +
                                "\"MATCH (n:" + Util.sanitizeAndQuote(label) + ") RETURN n\",\n" +
                                "\"SET n." + Util.sanitizeAndQuote(uuidConfig.getUuidProperty()) + " = " + uuidFunctionName + "()\", {batchSize:10000, parallel:true})")
                        .next()
        );
    }

    public static String getUuidFunctionName() {
        ApocConfig.UuidFormatType formatType = ApocConfig.apocConfig().getEnumProperty(ApocConfig.APOC_UUID_FORMAT, ApocConfig.UuidFormatType.class, ApocConfig.UuidFormatType.hex);
        switch(formatType) {
            case base64:
                return "apoc.create.uuidBase64";
            case hex:
            default:
                return "apoc.create.uuid";
        }
    }

}
