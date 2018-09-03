package apoc.uuid;

import apoc.ApocConfiguration;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author ab-larus
 * @since 05.09.18
 */

public class UUIDLifeCycle {

    public static final int DEFAULT_SCHEDULE = 60;
    public static JobScheduler.Group UUID_GROUP = new JobScheduler.Group("UUID");
    private final JobScheduler scheduler;
    private final GraphDatabaseAPI db;
    private JobScheduler.JobHandle uuidJobHandle;
    private JobScheduler.JobHandle uuidIndexJobHandle;
    private Log log;

    public UUIDLifeCycle(JobScheduler scheduler, GraphDatabaseAPI db, Log log) {
        this.scheduler = scheduler;
        this.log = log;
        this.db = db;
    }

    public void start() {
        boolean enabled = Util.toBoolean(ApocConfiguration.get("uuid.enabled", null));
        if (!enabled) return;

        long uuidSchedule = Util.toLong(ApocConfiguration.get("uuid.schedule", DEFAULT_SCHEDULE));

        String label = ApocConfiguration.get("uuid.labels", StringUtils.EMPTY);
        List<String> labels = Stream.of(label.split(",")).map(String::trim).collect(Collectors.toList());

        uuidIndexJobHandle = scheduler.schedule(UUID_GROUP, () -> createUUIDIndex(labels), (int)(uuidSchedule*0.8), TimeUnit.SECONDS);
        uuidJobHandle = scheduler.scheduleRecurring(UUID_GROUP, () -> setUUID(labels), uuidSchedule, uuidSchedule, TimeUnit.SECONDS);
    }

    private void createUUIDIndex(List<String> labels) {
        try {
            labels.forEach(label -> db.execute(String.format("CREATE CONSTRAINT ON (%s:%s) ASSERT %s.uuid IS UNIQUE", label.toLowerCase(),
                    label, label.toLowerCase())).close());
        } catch (Exception e) {
            log.error("UUID: Error creating index", e);
        }
    }

    public void setUUID(List<String> labels) {
        if (!Util.isWriteableInstance(db)) return;
        labels.forEach(label -> {
            try (Transaction tx = db.beginTx()) {
                try (ResourceIterator<Node> result = db.execute(String.format("MATCH (n:%s) WHERE not exists(n.uuid) RETURN n", label)).columnAs("n")) {
                    while (result.hasNext()) {
                        String uuid = UUID.randomUUID().toString();
                        Node node = result.next();
                        node.setProperty("uuid", uuid);
                    }
                }
                tx.success();
            } catch (Exception e) {
                log.error("UUID: Error adding uuid to Node", e);
            }
        });
    }

    public void stop() {
        if (uuidIndexJobHandle != null) uuidIndexJobHandle.cancel(true);
        if (uuidJobHandle != null) uuidJobHandle.cancel(true);
    }
}