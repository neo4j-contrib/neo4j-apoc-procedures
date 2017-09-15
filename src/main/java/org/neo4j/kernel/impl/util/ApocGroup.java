package org.neo4j.kernel.impl.util;

import org.neo4j.scheduler.JobScheduler;

/**
 * @author mh
 * @since 21.05.16
 */
public class ApocGroup extends JobScheduler.Group {
    public static JobScheduler.Group TTL_GROUP = new ApocGroup("TTL");

    public ApocGroup(String name) {
        super(name);
    }
}
