package apoc.monitor;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jmx.JmxUtils;

import javax.management.ObjectName;

public class Monitor {

    protected ObjectName getObjectName(GraphDatabaseService db, String name) {
        return JmxUtils.getObjectName(db, name);
    }

    protected Object getAttribute(ObjectName objectName, String attribute) {
        return JmxUtils.getAttribute(objectName, attribute);
    }

}
