package apoc.load;

import apoc.Extended;
import apoc.load.util.LoadJdbcConfig;
import apoc.result.RowResult;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.load.Jdbc.executeQuery;
import static apoc.load.Jdbc.executeUpdate;
import static apoc.load.util.JdbcUtil.getConnection;
import static apoc.load.util.JdbcUtil.getUrlOrKey;

@Extended
public class Analytics {
    public static final String PROVIDER_CONF_KEY = "provider";
    public static final String TABLE_NAME_CONF_KEY = "tableName";
    public static final String TABLE_NAME_DEFAULT_CONF_KEY = "neo4j_tmp_table";

    enum Provider {
        DEFAULT,
        POSTGRES,
        MYSQL
    }

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure("apoc.jdbc.analytics")
    @Description("apoc.jdbc.analytics(<cypherQuery>, <jdbcUrl>, <sqlQueryOverTemporaryTable>, <paramsList>, $config) - to create a temporary table starting from a Cypher query and delegate complex analytics to the database defined JDBC URL ")
    public Stream<RowResult> aggregate(
            @Name("neo4jQuery") String neo4jQuery,
            @Name("jdbc") String urlOrKey,
            @Name("sqlQuery") String sqlQuery,
            @Name(value = "params", defaultValue = "[]") List<Object> params,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        AtomicReference<String> createTable = new AtomicReference<>();
        final Provider provider = Provider.valueOf((String) config.getOrDefault(PROVIDER_CONF_KEY, Provider.DEFAULT.name()));
        final String tableName = (String) config.getOrDefault(TABLE_NAME_CONF_KEY, TABLE_NAME_DEFAULT_CONF_KEY);

        AtomicReference<String> columns = new AtomicReference<>();
        AtomicReference<String> queryInsert = new AtomicReference<>();
        
                db.executeTransactionally(neo4jQuery,
                Map.of(),
                result -> {
                    List<String> sqlValuesForQueryInsert = new ArrayList<>();
                    result.forEachRemaining(map -> {
                        
                        if (createTable.get() == null) {
                            String tempTableClause = getTempTableClause(map, provider, tableName);
                            createTable.set(tempTableClause);
                        }

                        // convert Neo4j row result to SQL row
                        final String row = getStreamSortedByKey(map)
                              //  .map(e -> addFieldToTempTable(e, sqlTypesForTempTable, provider))
                                .map(Map.Entry::getValue)
                                .map(Analytics::formatSqlValue)
                                .collect(Collectors.joining(","));
                        
                        // add SQL row for query insert
                        sqlValuesForQueryInsert.add("(" + row + ")");
                    });
                    
                    // add values to `INSERT INTO ...` clause
                    String sqlValues = StringUtils.join(sqlValuesForQueryInsert, ",");
                    String insertClause = String.format("INSERT INTO %s VALUES %s",
                            tableName, sqlValues
                    );
                    queryInsert.set(insertClause);
                    
                    // columns to handle error msg
                    String neo4jResultColumns = result.columns().stream()
                            .sorted()
                            .collect(Collectors.joining(","));
                    columns.set(neo4jResultColumns);
                    return null;
                });

        String url = getUrlOrKey(urlOrKey);
        LoadJdbcConfig jdbcConfig = new LoadJdbcConfig(config);
        Connection connection;
        try {
            connection = (Connection) getConnection(url, jdbcConfig, Connection.class);
        } catch (Exception e) {
            throw new RuntimeException("Connection error", e);
        }
        
        Object[] paramsArray = params.toArray(new Object[params.size()]);

        // Step 1. Create temporary table
        executeUpdate(urlOrKey, createTable.get(), config, connection, log, paramsArray);

        // Step 2. Insert data in temp table
        executeUpdate(urlOrKey, queryInsert.get(), config, connection, log, paramsArray);
        
        try {
            // Step 3. Return data from temp table
            return executeQuery(urlOrKey, sqlQuery, config, connection, log, paramsArray);
        } catch (Exception e) {
            String checkColConsistency = String.format("Make sure the SQL is consistent with Cypher query which has columns: %s", columns.get());
            throw new RuntimeException(checkColConsistency, e);
        }
    }

    /**
     * add fields to be added to the insert temp table clause
     * e.g. `CREATE TEMPORARY TABLE <tempTable> (tagline VARCHAR, language VARCHAR, title VARCHAR, released INTEGER)`
     */
    private String getTempTableClause(Map<String, Object> map, Provider provider, String tableName) {
        String sqlFields = getStreamSortedByKey(map)
                .map(e -> e.getKey() + " " + mapSqlType(provider, e.getValue()))
                .collect(Collectors.joining(","));

        return "CREATE TEMPORARY TABLE %s (%s)".formatted(tableName, sqlFields);
    }

    private static Stream<Map.Entry<String, Object>> getStreamSortedByKey(Map<String, Object> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey());
    }


    private static String formatSqlValue(Object x) {
        final String stringValue = x.toString();
        if (x instanceof Number) return stringValue;
        return String.format("'%s'", stringValue.replace("'", "''"));
    }

    private String mapSqlType(Provider provider, Object value) {
        return switch (provider) {
            case MYSQL, POSTGRES -> {
                if (value instanceof Number) yield "INTEGER";
                else yield "VARCHAR(1000)";
            }
            default -> {
                if (value instanceof Number) yield "INTEGER";
                else yield "VARCHAR";
            }
        };
    }
}
