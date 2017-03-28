package apoc.algo;

import apoc.Pools;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static apoc.util.Util.parseDirection;

public class LabelPropagation {
    static final ExecutorService pool = Pools.DEFAULT;

    @Context
    public GraphDatabaseService db;

    @Context
    public GraphDatabaseAPI dbAPI;

    @Context
    public Log log;

    @Procedure(name = "apoc.algo.community",mode = Mode.WRITE)
    @Description("CALL apoc.algo.community(times,labels,partitionKey,type,direction,weightKey,batchSize) - simple label propagation kernel")
    public void community(
            @Name("times") long times,
            @Name("labels") List<String> labelNames,
            @Name("partitionKey") String partitionKey,
            @Name("type") String relationshipTypeName,
            @Name("direction") String directionName,
            @Name("weightKey") String weightKey,
            @Name("batchSize") long batchSize
    ) throws ExecutionException {
        Set<Label> labels = labelNames == null ? Collections.emptySet() : new HashSet<>(labelNames.size());
        if (labelNames != null)
            labelNames.forEach(name -> labels.add(Label.label(name)));

        RelationshipType relationshipType = relationshipTypeName == null ? null : RelationshipType.withName(relationshipTypeName);

        Direction direction = parseDirection(directionName);

        for (int i = 0; i < times; i++) {
            List<Node> batch = null;
            List<Future<Void>> futures = new ArrayList<>();
            try (Transaction tx = dbAPI.beginTx()) {
                for (Node node : dbAPI.getAllNodes()) {
                    boolean add = labels.size() == 0;
                    if (!add) {
                        Iterator<Label> nodeLabels = node.getLabels().iterator();
                        while (!add && nodeLabels.hasNext()) {
                            if (labels.contains(nodeLabels.next()))
                                add = true;
                        }
                    }
                    if (add) {
                        if (batch == null) {
                            batch = new ArrayList<>((int) batchSize);
                        }
                        batch.add(node);
                        if (batch.size() == batchSize) {
                            futures.add(clusterBatch(batch, partitionKey, relationshipType, direction, weightKey));
                            batch = null;
                        }
                    }
                }
                if (batch != null) {
                    futures.add(clusterBatch(batch, partitionKey, relationshipType, direction, weightKey));
                }
                tx.success();
            }

            // Await processing of node batches
            for (Future<Void> future : futures) {
                Pools.force(future);
            }
        }
    }

    private Future<Void> clusterBatch(List<Node> batch, String partitionKey, RelationshipType relationshipType, Direction direction, String weightKey) {
        return Pools.processBatch(batch, dbAPI, (node) -> {
            Map<Object, Double> votes = new HashMap<>();
            for (Relationship rel :
                    relationshipType == null
                            ? node.getRelationships(direction)
                            : node.getRelationships(relationshipType, direction)) {
                Node other = rel.getOtherNode(node);
                Object partition = partition(other, partitionKey);
                double weight = weight(rel, weightKey) * weight(other, weightKey);
                vote(votes, partition, weight);
            }

            Object originalPartition = partition(node, partitionKey);
            Object partition = originalPartition;
            double weight = 0.0d;
            for (Map.Entry<Object, Double> entry : votes.entrySet()) {
                if (weight < entry.getValue()) {
                    weight = entry.getValue();
                    partition = entry.getKey();
                }
            }

            if (partition != originalPartition)
                node.setProperty(partitionKey, partition);
        });
    }

    private void vote(Map<Object, Double> votes, Object partition, double weight) {
        double currentWeight = votes.getOrDefault(partition, 0.0d);
        double newWeight = currentWeight + weight;
        votes.put(partition, newWeight);
    }

    private double weight(PropertyContainer container, String propertyKey) {
        if (propertyKey != null) {
            Object propertyValue = container.getProperty(propertyKey, null);
            if (propertyValue instanceof Number) {
                return ((Number) propertyValue).doubleValue();
            }
        }
        return 1.0d;
    }

    private Object partition(Node node, String partitionKey) {
        Object partition = node.getProperty(partitionKey, null);
        return partition == null ? node.getId() : partition;
    }
}
