package apoc.cypher;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Collections;
import java.util.Map;

import static apoc.cypher.Cypher.withParamMapping;

/**
 * Created by lyonwj on 9/29/17.
 */
public class CypherFunctions {
    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("apoc.cypher.runFirstColumn(kernelTransaction, params, expectMultipleValues) - executes kernelTransaction with given parameters, returns first column only, if expectMultipleValues is true will collect results into an array")
    public Object runFirstColumn(@Name("cypher") String statement, @Name("params") Map<String, Object> params, @Name(value = "expectMultipleValues",defaultValue = "true") boolean expectMultipleValues) {
        if (params == null) params = Collections.emptyMap();
        try (Result result = db.execute(withParamMapping(statement, params.keySet()), params)) {

        String firstColumn = result.columns().get(0);
        try (ResourceIterator<Object> iter = result.columnAs(firstColumn)) {
            if (expectMultipleValues) return iter.stream().toArray();
            return iter.hasNext() ? iter.next() : null;
        }
      }
    }
}
