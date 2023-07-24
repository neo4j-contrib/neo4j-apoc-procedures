package apoc.uuid;

import apoc.Extended;
import apoc.util.SystemDbUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.*;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.SystemDbUtil.withSystemDb;
import static apoc.uuid.UUIDHandlerNewProcedures.checkEnabled;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;

@Extended
public class UUIDNewProcedures {
    public static final String UUID_NOT_SET = APOC_UUID_REFRESH + " is not set. Please please set it in your apoc.conf";

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    private void checkInSystemLeader(String databaseName) {
        checkEnabled(databaseName);
        checkRefreshConfigSet();

        SystemDbUtil.checkInSystemLeader(db);
    }

    private void checkTargetDatabase(String databaseName) {
        SystemDbUtil.checkTargetDatabase(tx, databaseName, "Automatic UUIDs");
    }

    private void checkRefreshConfigSet() {
        if (!apocConfig().getConfig().containsKey(APOC_UUID_REFRESH)) {
            throw new RuntimeException(UUID_NOT_SET);
        }
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.setup(label, databaseName, $config) | eventually adds the uuid transaction handler for the provided `label` and `uuidProperty`, in case the UUID handler is already present it will be replaced by the new one")
    public Stream<UuidInfo> setup(@Name("label") String label,
                                  @Name(value = "databaseName", defaultValue = "neo4j") String databaseName,
                                  @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        checkInSystemLeader(databaseName);
        checkTargetDatabase(databaseName);

        UuidConfig uuidConfig = new UuidConfig(config);

        // unlike the apoc.uuid.install we don't return the UuidInstallInfo because we don't retrieve the `batchComputationResult` field
        UuidInfo uuidInfo = UUIDHandlerNewProcedures.create(databaseName, label, uuidConfig);
        return Stream.of(uuidInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.drop(label, databaseName) yield label, installed, properties | eventually removes previously added UUID handler and returns uuid information")
    public Stream<UuidInfo> drop(@Name("label") String label, @Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkInSystemLeader(databaseName);

        final UuidInfo uuidInfo = UUIDHandlerNewProcedures.drop(databaseName, label);
        return Stream.ofNullable(uuidInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.dropAll(databaseName) yield label, installed, properties | eventually removes all previously added UUID handlers and returns uuids' information")
    public Stream<UuidInfo> dropAll(@Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkInSystemLeader(databaseName);

        return UUIDHandlerNewProcedures.dropAll(databaseName)
                .stream()
                .sorted(Comparator.comparing(i -> i.label));
    }

    // not to change with @SystemOnlyProcedure because this procedure can be executed in user dbs as well
    // since is a read-only operation
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.uuid.show(databaseName) | it lists all eventually installed UUID handler for a database")
    public Stream<UuidInfo> show(@Name(value = "databaseName", defaultValue = "neo4j") String databaseName) {
        checkEnabled(databaseName);

        return withSystemDb(sysTx -> {
            return UUIDHandlerNewProcedures.getUuidNodes(sysTx, databaseName)
                    .stream()
                    .map(UuidInfo::new)
                    .collect(Collectors.toList());
        }).stream();
    }

}
