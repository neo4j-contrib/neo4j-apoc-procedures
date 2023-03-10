package apoc.uuid;

import apoc.Extended;
import apoc.util.SystemDbUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.*;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;

@Extended
public class UUIDNewProcedures {
    public static final String UUID_NOT_SET = APOC_UUID_REFRESH + " is not set. Please please set it in your apoc.conf";

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    private void checkInSystemLeader(String databaseName) {
        UUIDHandlerNewProcedures.checkEnabled(databaseName);
        checkRefreshConfigSet();

        SystemDbUtil.checkInSystemLeader(db);
    }

    private void checkInSystem(String databaseName) {
        UUIDHandlerNewProcedures.checkEnabled(databaseName);
        SystemDbUtil.checkInSystem(db);
    }

    private void checkTargetDatabase(String databaseName) {
        SystemDbUtil.checkTargetDatabase(databaseName, "Automatic UUIDs");
    }

    private void checkRefreshConfigSet() {
        if (!apocConfig().containsKey(APOC_UUID_REFRESH)) {
            throw new RuntimeException(UUID_NOT_SET);
        }
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.install(databaseName, label, $config) | eventually adds the uuid transaction handler for the provided `label` and `uuidProperty`, in case the UUID handler is already present it will be replaced by the new one")
    public Stream<UuidInfo> create(@Name("databaseName") String databaseName,
                                   @Name("label") String label,
                                   @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        checkInSystemLeader(databaseName);
        checkTargetDatabase(databaseName);


        UuidConfig uuidConfig = new UuidConfig(config);

        // TODO - the ApocConfig.apocConfig().getDatabase(databaseName) has to be deleted in 5.x,
        //  because in a cluster, not all DBMS host all the databases on them,
        //  so we have to assume that the leader of the system database doesn't have access to this user database.
        //  Maybe we could put it in UuidHandler.java and execute it in the refresh() method before all
        GraphDatabaseService db = apocConfig().getDatabase(databaseName);
        try (Transaction tx = db.beginTx()) {
            UUIDHandlerNewProcedures.checkConstraintUuid(tx, label, uuidConfig.getUuidProperty());
            tx.commit();
        }

        // unlike the apoc.uuid.install we don't return the UuidInstallInfo because we don't retrieve the `batchComputationResult` field
        UuidInfo uuidInfo = UUIDHandlerNewProcedures.create(databaseName, label, uuidConfig);
        return Stream.of(uuidInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.drop(databaseName, label) yield label, installed, properties | eventually removes previously added UUID handler and returns uuid information")
    public Stream<UuidInfo> drop(@Name("databaseName") String databaseName, @Name("label") String label) {
        checkInSystemLeader(databaseName);

        final UuidInfo uuidInfo = UUIDHandlerNewProcedures.drop(databaseName, label);
        return Stream.ofNullable(uuidInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.uuid.dropAll(databaseName) yield label, installed, properties | eventually removes all previously added UUID handlers and returns uuids' information")
    public Stream<UuidInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystemLeader(databaseName);

        return UUIDHandlerNewProcedures.dropAll(databaseName)
                .stream()
                .sorted(Comparator.comparing(i -> i.label));
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.uuid.show(databaseName) | it lists all eventually installed UUID handler for a database")
    public Stream<UuidInfo> show(@Name("databaseName") String databaseName) {
        checkInSystem(databaseName);

        return UUIDHandlerNewProcedures.getUuidNodes(tx, databaseName)
                .stream()
                .map(UuidInfo::new);
    }

}
