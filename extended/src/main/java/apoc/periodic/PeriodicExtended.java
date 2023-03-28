package apoc.periodic;

import apoc.Description;
import apoc.Extended;
import apoc.Pools;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.SCHEMA_WRITE;

@Extended
public class PeriodicExtended {

    @Context
    public GraphDatabaseService db;
    
    @Context
    public Log log;
    
    @Context
    public Pools pools;

    @Procedure(mode = Mode.SCHEMA)
    @Description("apoc.periodic.submitSchema(name, statement, $config) - equivalent to apoc.periodic.submit which can also accept schema operations")
    public Stream<PeriodicUtils.JobInfo> submitSchema(@Name("name") String name, @Name("statement") String statement, @Name(value = "params", defaultValue = "{}") Map<String,Object> config) {
        validateQuery(statement);
        return PeriodicUtils.submitProc(name, statement, config, db, log, pools);
    }
    
    private void validateQuery(String statement) {
        Util.validateQuery(db, statement,
                READ_ONLY, WRITE, READ_WRITE, SCHEMA_WRITE);
    }

}
