package apoc.load.jdbc;

import apoc.Extended;
import apoc.load.util.LoadJdbcConfig;
import apoc.result.RowResult;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.sql.Connection;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.load.jdbc.Jdbc.executeQuery;
import static apoc.load.jdbc.Jdbc.executeUpdate;
import static apoc.load.util.JdbcUtil.*;

@Extended
public class Analytics {
    public static final String PROVIDER_CONF_KEY = "provider";
    public static final String TABLE_NAME_CONF_KEY = "tableName";
    public static final String TABLE_NAME_DEFAULT_CONF_KEY = "neo4j_tmp_table";

    public enum Provider {
        DUCKDB(DUCK_TYPE_MAP, "\"%s\" %s"),
        POSTGRES(POSTGRES_TYPE_MAP, "\"%s\" %s"),
        MYSQL(MYSQL_TYPE_MAP, "`%s` %s");
        
        public final Map<Class<?>, String> typeMap;
        public final String tableTypeTemplate;
        Provider(Map<Class<?>, String> typeMap, String tableTypeTemplate) {
            this.typeMap = typeMap;
            this.tableTypeTemplate = tableTypeTemplate;
        }

        public String byteArrayToBlobString(byte[] data) {
            if (data == null) {
                throw new IllegalArgumentException("Data and database type must not be null");
            }

            // Convert byte array to a hexadecimal string digestible by the DBMS
            String hexString = Hex.encodeHexString(data);

            return switch (this) {
                case DUCKDB:
                    yield "X'%s'".formatted(hexString);
                case MYSQL:
                    yield "0x" + hexString;
                case POSTGRES:
                    yield "decode('%s', 'hex')".formatted(hexString);
            };
        }
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
        final Provider provider = Provider.valueOf((String) config.getOrDefault(PROVIDER_CONF_KEY, Provider.DUCKDB.name()));
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
                                .map(Map.Entry::getValue)
                                .map(i -> Analytics.formatSqlValue(i, provider))
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
                .map(e -> {
                    String type = mapSqlType(provider, e.getValue());
                    return provider.tableTypeTemplate.formatted(e.getKey(), type);
                })
                .collect(Collectors.joining(","));

        return "CREATE TEMPORARY TABLE %s (%s)".formatted(tableName, sqlFields);
    }

    private static Stream<Map.Entry<String, Object>> getStreamSortedByKey(Map<String, Object> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey());
    }

    private static String formatSqlValue(Object val, Provider provider) {
        String stringValue = val.toString();
        if (val instanceof Number) {
            return stringValue;
        }
        if (val instanceof ZonedDateTime zonedDateTime) {
            stringValue = toSqlCompatibleDateTime(zonedDateTime);
        }
        if (val instanceof OffsetTime zonedDateTime) {
            stringValue = toSqlCompatibleTimeFormat(zonedDateTime);
        }
        if (val instanceof byte[] bytes) {
            stringValue = provider.byteArrayToBlobString(bytes);
        }
        return String.format("'%s'", stringValue.replace("'", "''"));
    }
    
    private String mapSqlType(Provider provider, Object value) {
        if (value == null) {
            return VARCHAR_TYPE;
        }
        Class<?> clazz = value.getClass();
        return provider.typeMap.getOrDefault(clazz, VARCHAR_TYPE);
    }
}
