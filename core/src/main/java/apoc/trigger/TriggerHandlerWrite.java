package apoc.trigger;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class TriggerHandlerWrite {
    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public static Map<String, Object> toTriggerInfo(Node node) {
        return node.getAllProperties()
                .entrySet().stream()
                .filter(e -> !List.of(SystemPropertyKeys.name.name(), SystemPropertyKeys.database.name()).contains(e.getKey()))
                .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                        (mapAccumulator, e) -> {
                            Object value = List.of(SystemPropertyKeys.selector.name(), SystemPropertyKeys.params.name()).contains(e.getKey()) 
                                    ? Util.fromJson((String) e.getValue(), Map.class) 
                                    : e.getValue();
                            
                            mapAccumulator.put(e.getKey(), value);
                    }, HashMap::putAll);
    }

    private static boolean isEnabled() {
        return apocConfig().getBoolean(APOC_TRIGGER_ENABLED);
    }

    public static void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public static Map<String, Object> install(String databaseName, String triggerName, String statement, Map<String,Object> selector, Map<String,Object> params) {
        final HashMap<String, Object> previous = new HashMap<>();

        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                    Pair.of(SystemPropertyKeys.database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.name.name(), triggerName));
            
            // we'll return previous trigger info
            previous.putAll(TriggerHandlerWrite.toTriggerInfo(node));
            
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);

            setLastUpdate(databaseName, tx);
            return null;
        });

        return previous;
    }

    public static Map<String, Object> drop(String databaseName, String triggerName) {
        final HashMap<String, Object> previous = new HashMap<>();

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx, triggerName)
                    .forEachRemaining(node -> {
                                previous.putAll(TriggerHandlerWrite.toTriggerInfo(node));
                                node.delete();
                            });
            
            setLastUpdate(databaseName, tx);
            return null;
        });

        return previous;
    }

    public static Map<String, Object> updatePaused(String databaseName, String name, boolean paused) {
        HashMap<String, Object> result = new HashMap<>();

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx, name)
                    .forEachRemaining(node -> {
                        node.setProperty( SystemPropertyKeys.paused.name(), paused );

                        // we'll return previous trigger info
                        result.putAll(TriggerHandlerWrite.toTriggerInfo(node));
                    });

            setLastUpdate(databaseName, tx);
            return null;
        });

        return result;
    }

    public static Map<String, Object> dropAll(String databaseName) {
        HashMap<String, Object> previous = new HashMap<>();

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx)
                    .forEachRemaining(node -> {
                        String triggerName = (String) node.getProperty(SystemPropertyKeys.name.name());

                        // we'll return previous trigger info
                        previous.put(triggerName, TriggerHandlerWrite.toTriggerInfo(node));
                        node.delete();
                    });
            setLastUpdate(databaseName, tx);
            return null;
        });

        return previous;
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

    public static <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    private static void setLastUpdate(String databaseName, Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName);
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocTriggerMeta);
            node.setProperty(SystemPropertyKeys.database.name(), databaseName);
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }
    
}
