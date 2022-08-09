package apoc.cypher;

import apoc.result.MapResult;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Name;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;

public class CypherUtils {
    public static Stream<MapResult> runCypherQuery(Transaction tx, @Name("cypher") String statement, @Name("params") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        return tx.execute(withParamMapping(statement, params.keySet()), params).stream().map(MapResult::new);
    }

    public static String withParamMapping(String fragment, Collection<String> keys) {
        if (keys.isEmpty()) return fragment;
        String declaration = " WITH " + join(", ", keys.stream().map(s -> format(" $`%s` as `%s` ", s, s)).collect(toList()));
        return declaration + fragment;
    }

}
