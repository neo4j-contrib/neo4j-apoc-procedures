package apoc.util;

import apoc.SystemPropertyKeys;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.SystemPropertyKeys.database;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDbUtil {
    public static final String NON_SYS_DB_ERROR = "The procedure should be executed against a system database.";
    public static final String SYS_NON_LEADER_ERROR = "It's not possible to write into a cluster member with a non-LEADER system database.\n";
    public static final String PROCEDURE_NOT_ROUTED_ERROR = "No write operations are allowed directly on this database. " +
            "Writes must pass through the leader. The role of this server is: FOLLOWER";
    public static final String DB_NOT_FOUND_ERROR = "The user database with name '%s' does not exist";

    public static final String BAD_TARGET_ERROR = " can only be installed on user databases.";


    /**
     * Check that the system database can write,
     * otherwise throws an error advising to switch to the new procedures as these are deprecated
     *
     * @param msgDeprecation
     */
    public static void checkWriteAllowed(String msgDeprecation) {
        GraphDatabaseAPI sysDb = (GraphDatabaseAPI) apocConfig().getSystemDb();
        if (!Util.isWriteableInstance(sysDb)) {
            throw new RuntimeException(SYS_NON_LEADER_ERROR + msgDeprecation);
        }
    }

    /**
     * Check that the database name is equal to "system"
     */
    public static void checkInSystemDb(GraphDatabaseService db) {
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(NON_SYS_DB_ERROR);
        }
    }

    /**
     * Check that the database name is equal to "system" and the system database can write
     *
     * @param db
     */
    public static void checkInSystemLeader(GraphDatabaseService db) {
        GraphDatabaseAPI sysDb = (GraphDatabaseAPI) apocConfig().getSystemDb();
        // routing check
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(sysDb)) {
            throw new RuntimeException(PROCEDURE_NOT_ROUTED_ERROR);
        }
    }

    /**
     * Check that the database exists and is not equal to "system"
     *
     * @param databaseName
     * @param type: the procedure type
     */
    public static void checkTargetDatabase(Transaction tx, String databaseName, String type) {
        final Set<String> databases = tx.execute("SHOW DATABASES", Collections.emptyMap())
                .<String>columnAs("name")
                .stream()
                .collect(Collectors.toSet());
        if (!databases.contains(databaseName)) {
            throw new RuntimeException( String.format(DB_NOT_FOUND_ERROR, databaseName) );
        }

        if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(type + BAD_TARGET_ERROR);
        }
    }

    /**
     * Creates a system db transaction and returns the Function result after the commit
     *
     * @param action: the system db operation
     */
    public static <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    /**
     * Creates a system db transaction which returns a void
     *
     * @param consumer: the system db operation
     */
    public static void withSystemDb(Consumer<Transaction> consumer) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
    }

    /**
     * Given a label and the property `database`, retrieves the system nodes
     * If the Map `props` is not null, only retrieves nodes with the specified properties
     *
     * @param tx: the current system transaction
     * @param databaseName
     * @param sysLabel
     * @param props: required property key-value combinations
     * @return
     */
    public static ResourceIterator<Node> getSystemNodes(Transaction tx,
                                                        String databaseName,
                                                        Label sysLabel,
                                                        Map<String, Object> props) {
            final String dbNameKey = database.name();

            // search all system nodes
            if (props == null) {
                return tx.findNodes(sysLabel, dbNameKey, databaseName);
            }

            // search system nodes specified by some system prop keys, like name or label
            Map<String, Object> propsMap = new HashMap<>();
            propsMap.put(dbNameKey, databaseName);
            propsMap.putAll(props);

            return tx.findNodes(sysLabel, propsMap);
    }

    /**
     * Given a label and a property with key database and value `databaseName`,
     * or gets the node if it already exists,
     * and creates a system node and set the property lastUpdated to System.currentTimeMillis()
     *
     * @param tx
     * @param databaseName
     * @param label: the system label
     */
    public static void setLastUpdate(Transaction tx, String databaseName, Label label) {
        Node node = tx.findNode(label, database.name(), databaseName);
        if (node == null) {
            node = tx.createNode(label);
            node.setProperty(database.name(), databaseName);
        }
        final long value = System.currentTimeMillis();
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), value);
    }

    /**
     * Gets the property-value with key `lastUpdated`,
     * where the key database is equal to `databaseName` and the label is equal to `label`
     *
     * @param databaseName: the value of `database` key
     * @param label: the system label
     * @return
     */
    public static long getLastUpdate(String databaseName, Label label) {
        return SystemDbUtil.withSystemDb(tx -> {
            Node node = tx.findNode(label, SystemPropertyKeys.database.name(), databaseName);
            return node == null
                    ? 0L
                    : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
        });
    }
}
