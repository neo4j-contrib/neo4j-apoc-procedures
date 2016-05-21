package org.neo4j.kernel.impl.util;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

/**
 * @author mh
 * @since 21.05.16
 */
public class ApocGroup extends JobScheduler.Group {
    public static JobScheduler.Group TTL_GROUP = new ApocGroup("TTL", POOLED);

    public ApocGroup(String name, JobScheduler.SchedulingStrategy strategy) {
        super(name, strategy);
    }
}
