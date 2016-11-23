package apoc.monitor;

import apoc.Description;
import apoc.result.ConfigInfoResult;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Config {

    private static final String PROCEDURE_NAME = "org.neo4j:instance=kernel#0,name=Configuration";

    @Context
    public GraphDatabaseService database;

    @Procedure
    @Description("apoc.monitor.config() returns the configuration parameters used to configure Neo4j")
    public Stream<ConfigInfoResult> config() {
        List<ConfigInfoResult> attributes = new ArrayList<>();
        try (org.neo4j.graphdb.Transaction tx = database.beginTx()) {
            Result result = database.execute("CALL dbms.queryJmx(\"" + PROCEDURE_NAME + "\")");
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                Map<String, Object> attr = (Map<String, Object>) record.get("attributes");
                for (String k : attr.keySet()) {
                    Map<String, Object> v = (Map<String, Object>) attr.get(k);
                    attributes.add(new ConfigInfoResult(k, String.valueOf(v.get("value")), String.valueOf(v.get("description"))));
                }

            }
            tx.success();
        }

        return attributes.stream().map(p -> p);
    }
}
