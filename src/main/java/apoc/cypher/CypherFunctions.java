package apoc.cypher;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import java.util.Iterator;


import static apoc.cypher.Cypher.withParamMapping;

/**
 * Created by lyonwj on 9/29/17.
 */
public class CypherFunctions {
    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("apoc.cypher.runFirstColumn(statement, params, expectMultipleValues) - executes statement with given parameters, returns first column only, if expectMultipleValues is true will collect results into an array")
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

    @Procedure(name = "apoc.cypher.runFirstColumnWrites")
    @PerformsWrites
    @Description("apoc.cypher.runFirstColumnWrites(statement, params) - executes write statement with given parameters, yields first column only (nodes)")
    public Stream<NodeResult> runFirstColumnWrites(@Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        try (Result result = db.execute(withParamMapping(statement, params.keySet()), params)) {
            String firstColumn = result.columns().get(0);
            ResourceIterator<Node> resultColumn = result.columnAs(firstColumn);
            return resultColumn.stream().map( item -> new NodeResult(item));
        }
    }

    public class NodeResult {

        public final Node node;

        public NodeResult(Node node) {
            this.node = node;
        }
    }
}
