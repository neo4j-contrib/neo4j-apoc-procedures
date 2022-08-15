package apoc.bolt;

import apoc.Extended;
import apoc.result.RowResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author AgileLARUS
 * @since 29.08.17
 */
@Extended
public class Bolt {

    @Context
    public GraphDatabaseService db;

    private <T> T withConnection(String url, Map<String, Object> config, Function<BoltConnection, T> action) throws URISyntaxException {
        BoltConnection connection = BoltConnection.from(config, url);
        return action.apply(connection);
    }
    
    @Procedure()
    @Description("apoc.bolt.load(url-or-key, kernelTransaction, params, config) - access to other databases via bolt for read")
    public Stream<RowResult> load(@Name("url") String url, @Name("kernelTransaction") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        return withConnection(url, config, 
                conn -> conn.loadFromSession(statement, params));
    }

    @Procedure(value = "apoc.bolt.load.fromLocal", mode = Mode.WRITE)
    public Stream<RowResult> fromLocal(@Name("url") String url,
                                       @Name("localStatement") String localStatement,
                                       @Name("remoteStatement") String remoteStatement,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config)  throws URISyntaxException {
        return withConnection(url, config, 
                conn -> conn.loadFromLocal(localStatement, remoteStatement, db));
    }

    @Procedure()
    @Description("apoc.bolt.execute(url-or-key, kernelTransaction, params, config) - access to other databases via bolt for reads and writes")
    public Stream<RowResult> execute(@Name("url") String url, @Name("kernelTransaction") String statement, @Name(value = "params", defaultValue = "{}") Map<String, Object> params, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws URISyntaxException {
        Map<String, Object> configuration = new HashMap<>(config);
        configuration.put("readOnly", false);
        return load(url, statement, params, configuration);
    }

}