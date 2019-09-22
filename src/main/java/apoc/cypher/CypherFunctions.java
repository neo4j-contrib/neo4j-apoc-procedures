package apoc.cypher;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.cypher.Cypher.withParamMapping;

/**
 * Created by lyonwj on 9/29/17.
 */
public class CypherFunctions {
    @Context
    public Transaction tx;

    @UserFunction
    @Deprecated
    @Description("use either apoc.cypher.runFirstColumnMany for a list return or apoc.cypher.runFirstColumnSingle for returning the first row of the first column")
    public Object runFirstColumn(@Name("cypher") String statement, @Name("params") Map<String, Object> params, @Name(value = "expectMultipleValues",defaultValue = "true") boolean expectMultipleValues) {
        if (params == null) params = Collections.emptyMap();
        String resolvedStatement = withParamMapping(statement, params.keySet());
        if (!resolvedStatement.contains(" runtime")) resolvedStatement = "cypher runtime=slotted " + resolvedStatement;
        try (Result result = tx.execute(resolvedStatement, params)) {

        String firstColumn = result.columns().get(0);
        try (ResourceIterator<Object> iter = result.columnAs(firstColumn)) {
            if (expectMultipleValues) return iter.stream().collect(Collectors.toList());
            return iter.hasNext() ? iter.next() : null;
        }
      }
    }

    @UserFunction
    @Description("apoc.cypher.runFirstColumnMany(statement, params) - executes statement with given parameters, returns first column only collected into a list, params are available as identifiers")
    public List<Object> runFirstColumnMany(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return (List)runFirstColumn(statement, params, true);
    }
    @UserFunction
    @Description("apoc.cypher.runFirstColumnSingle(statement, params) - executes statement with given parameters, returns first element of the first column only, params are available as identifiers")
    public Object runFirstColumnSingle(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        return runFirstColumn(statement, params, false);
    }
}
