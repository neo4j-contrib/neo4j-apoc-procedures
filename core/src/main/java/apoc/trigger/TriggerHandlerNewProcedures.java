package apoc.trigger;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class TriggerHandlerNewProcedures {
    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    private static Map<String, Object> toTriggerInfo(Node node) {
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

        final Node[] node = new Node[1];
        withSystemDb(tx -> {
            node[0] = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                    Pair.of(SystemPropertyKeys.database.name(), databaseName),
                    Pair.of(SystemPropertyKeys.name.name(), triggerName));
            
            // we'll return previous trigger info
            previous.putAll(toTriggerInfo(node[0]));
            
            node[0].setProperty(SystemPropertyKeys.statement.name(), statement);
            node[0].setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node[0].setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node[0].setProperty(SystemPropertyKeys.paused.name(), false);

            return null;
        });

        setLastUpdate(databaseName, node[0]);

        return previous;
    }

    public static Map<String, Object> drop(String databaseName, String triggerName) {
        final HashMap<String, Object> previous = new HashMap<>();

        withSystemDb(tx -> {
            // there should be at most one node 
            final Node node = Iterators.singleOrNull(getTriggerNodes(databaseName, tx, triggerName));
            if (node != null) {
                previous.putAll(toTriggerInfo(node));
                node.delete();
            }
            
            return null;
        });

        setLastUpdate(databaseName, null);

        return previous;
    }

    public static Map<String, Object> updatePaused(String databaseName, String name, boolean paused) {
        HashMap<String, Object> result = new HashMap<>();

        final Node[] node = new Node[1];
        withSystemDb(tx -> {
            // there should be at most one node 
            node[0] = Iterators.singleOrNull(getTriggerNodes(databaseName, tx, name));
            if (node[0] != null) {
                node[0].setProperty(SystemPropertyKeys.paused.name(), paused);

                // we'll return previous trigger info
                result.putAll(toTriggerInfo(node[0]));
            }
            return null;
        });

        if (node[0] != null) setLastUpdate(databaseName, node[0]);

        return result;
    }

    public static Map<String, Object> dropAll(String databaseName) {
        HashMap<String, Object> previous = new HashMap<>();

        withSystemDb(tx -> {
            getTriggerNodes(databaseName, tx)
                    .forEachRemaining(node -> {
                        String triggerName = (String) node.getProperty(SystemPropertyKeys.name.name());

                        // we'll return previous trigger info
                        previous.put(triggerName, toTriggerInfo(node));
                        node.delete();
                    });
            return null;
        });

        setLastUpdate(databaseName, null);

        return previous;
    }

    public static List<Map<String, Object>> getTriggerNodes(String databaseName) {
        return withSystemDb(tx -> getTriggerNodes(databaseName, tx, null)
                .stream()
                .map(TriggerHandlerNewProcedures::toTriggerInfo)
                .collect(Collectors.toList())
        );
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

    private static void setLastUpdate(String databaseName, Node triggerNode) {
        withSystemDb(tx -> {
            Node metaNode = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName);
            if (metaNode == null) {
                metaNode = tx.createNode(SystemLabels.ApocTriggerMeta);
                metaNode.setProperty(SystemPropertyKeys.database.name(), databaseName);
            }
            metaNode.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());

            // we update the lastUpdated in ApocTrigger node as well
            if (triggerNode != null) {
                final Node nodeRebound = Util.rebind(tx, triggerNode);
                final Object lastUpdated = metaNode.getProperty(SystemPropertyKeys.lastUpdated.name());
                nodeRebound.setProperty(SystemPropertyKeys.lastUpdated.name(), lastUpdated);
            }
            return null;
        });
    }

    public static <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig().getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }
    
}
