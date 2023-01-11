package apoc.trigger;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.trigger.TriggerInfo.fromNode;

public class TriggerHandlerNewProcedures {
    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";


    private static boolean isEnabled() {
        return apocConfig().getBoolean(APOC_TRIGGER_ENABLED);
    }

    public static void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public static TriggerInfo install(String databaseName, String triggerName, String statement, Map<String,Object> selector, Map<String,Object> params) {
        final TriggerInfo[] previous = new TriggerInfo[1];

        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                    Pair.of(SystemPropertyKeys.database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.name.name(), triggerName));
            
            // we'll return previous trigger info
            previous[0] = fromNode(node, true);
            
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);

            setLastUpdate(databaseName, tx);
        });

        return previous[0];
    }

    public static TriggerInfo drop(String databaseName, String triggerName) {
        final TriggerInfo[] previous = new TriggerInfo[1];

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx, triggerName)
                    .forEachRemaining(node -> {
                                previous[0] = fromNode(node, false);
                                node.delete();
                            });
            
            setLastUpdate(databaseName, tx);
        });

        return previous[0];
    }

    public static TriggerInfo updatePaused(String databaseName, String name, boolean paused) {
        final TriggerInfo[] result = new TriggerInfo[1];

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx, name)
                    .forEachRemaining(node -> {
                        node.setProperty( SystemPropertyKeys.paused.name(), paused );

                        // we'll return previous trigger info
                        result[0] = fromNode(node, true);
                    });

            setLastUpdate(databaseName, tx);
        });

        return result[0];
    }

    public static List<TriggerInfo> dropAll(String databaseName) {
        final List<TriggerInfo> previous = new ArrayList<>();

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx)
                    .forEachRemaining(node -> {
                        String triggerName = (String) node.getProperty(SystemPropertyKeys.name.name());

                        // we'll return previous trigger info
                        previous.add( fromNode(node, false) );
                        node.delete();
                    });
            setLastUpdate(databaseName, tx);
        });

        return previous;
    }

    public static Stream<TriggerInfo> getTriggerNodesList(String databaseName, Transaction tx) {
        return getTriggerNodes(databaseName, tx)
                .stream()
                .map(trigger -> fromNode(trigger, true));
    }

    public static ResourceIterator<Node> getTriggerNodes(String databaseName, Transaction tx) {
        return getTriggerNodes(databaseName, tx, null);
    }
    
    public static ResourceIterator<Node> getTriggerNodes(String databaseName, Transaction tx, String name) {
        final SystemLabels label = SystemLabels.ApocTrigger;
        final String dbNameKey = SystemPropertyKeys.database.name();
        if (name == null) {
            return tx.findNodes(label, dbNameKey, databaseName);
        }
        return tx.findNodes(label, dbNameKey, databaseName,
                SystemPropertyKeys.name.name(), name);
    }

    public static void withSystemDb(Consumer<Transaction> consumer) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
    }

    private static void setLastUpdate(String databaseName, Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName);
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocTriggerMeta);
            node.setProperty(SystemPropertyKeys.database.name(), databaseName);
        }
        final long value = System.currentTimeMillis();
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), value);
    }
    
}
