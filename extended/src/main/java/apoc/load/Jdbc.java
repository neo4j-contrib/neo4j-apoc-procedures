package apoc.load;

import apoc.Extended;
import apoc.load.util.LoadJdbcConfig;
import apoc.result.RowResult;
import apoc.util.MapUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.load.util.JdbcUtil.*;

/**
 * @author mh
 * @since 26.02.16
 */
@Extended
public class Jdbc {

/*
    static {
        ApocConfiguration.get("jdbc").forEach((k, v) -> {
            if (k.endsWith("driver")) loadDriver(v.toString());
        });
    }
*/

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.load.driver('org.apache.derby.jdbc.EmbeddedDriver') register JDBC driver of source database")
    public void driver(@Name("driverClass") String driverClass) {
        loadDriver(driverClass);
    }

    public static void loadDriver(@Name("driverClass") String driverClass) {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load driver class "+driverClass+" "+e.getMessage());
        }
    }

    @Procedure(mode = Mode.READ)
    @Description("apoc.load.jdbc('key or url','table or statement', params, config) YIELD row - load from relational database, from a full table or a sql statement")
    public Stream<RowResult> jdbc(@Name("jdbc") String urlOrKey, @Name("tableOrSql") String tableOrSelect, @Name
            (value = "params", defaultValue = "[]") List<Object> params, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        params = params != null ? params : Collections.emptyList();
        return executeQuery(urlOrKey, tableOrSelect, config, params.toArray(new Object[params.size()]));
    }

    private Stream<RowResult> executeQuery(String urlOrKey, String tableOrSelect, Map<String, Object> config, Object... params) {
        LoadJdbcConfig loadJdbcConfig = new LoadJdbcConfig(config);
        String url = getUrlOrKey(urlOrKey);
        String query = getSqlOrKey(tableOrSelect);
        try {
            Connection connection = getConnection(url,loadJdbcConfig).get();
            // see https://jdbc.postgresql.org/documentation/91/query.html#query-with-cursors
            connection.setAutoCommit(loadJdbcConfig.isAutoCommit());
            try {
                PreparedStatement stmt = connection.prepareStatement(query,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(loadJdbcConfig.getFetchSize().intValue());
                try {
                    for (int i = 0; i < params.length; i++) stmt.setObject(i + 1, params[i]);
                    ResultSet rs = stmt.executeQuery();
                    Iterator<Map<String, Object>> supplier = new ResultSetIterator(log, rs, true, loadJdbcConfig);
                    Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
                    return StreamSupport.stream(spliterator, false)
                            .map(RowResult::new)
                            .onClose(() -> closeIt(log, stmt, connection));
                } catch (Exception sqle) {
                    closeIt(log, stmt);
                    throw sqle;
                }
            } catch(Exception sqle) {
                closeIt(log, connection);
                throw sqle;
            }
        } catch (Exception e) {
            log.error(String.format("Cannot execute SQL statement `%s`.%nError:%n%s", query, e.getMessage()),e);
            String errorMessage = "Cannot execute SQL statement `%s`.%nError:%n%s";
            if(e.getMessage().contains("No suitable driver")) errorMessage="Cannot execute SQL statement `%s`.%nError:%n%s%n%s";
            throw new RuntimeException(String.format(errorMessage, query, e.getMessage(), "Please download and copy the JDBC driver into $NEO4J_HOME/plugins,more details at https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_load_jdbc_resources"), e);
        }
    }

    @Procedure(mode = Mode.READ)
    @Description("apoc.load.jdbcUpdate('key or url','statement',[params],config) YIELD row - update relational database, from a SQL statement with optional parameters")
    public Stream<RowResult> jdbcUpdate(@Name("jdbc") String urlOrKey, @Name("query") String query, @Name(value = "params", defaultValue = "[]") List<Object> params,  @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        log.info( String.format( "Executing SQL update: %s", query ) );
        return executeUpdate(urlOrKey, query, config, params.toArray(new Object[params.size()]));
    }

    private Stream<RowResult> executeUpdate(String urlOrKey, String query, Map<String, Object> config, Object...params) {
        String url = getUrlOrKey(urlOrKey);
        LoadJdbcConfig jdbcConfig = new LoadJdbcConfig(config);
        try {
            Connection connection = getConnection(url,jdbcConfig).get();
            try {
                PreparedStatement stmt = connection.prepareStatement(query,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(5000);
                try {
                    for (int i = 0; i < params.length; i++) stmt.setObject(i + 1, params[i]);
                    int updateCount = stmt.executeUpdate();
                    closeIt(log, stmt, connection);
                    Map<String, Object> result = MapUtil.map("count", updateCount);
                    return Stream.of(result)
                            .map(RowResult::new);
                } catch(Exception sqle) {
                    closeIt(log,stmt);
                    throw sqle;
                }
            } catch(Exception sqle) {
                closeIt(log,connection);
                throw sqle;
            }
        } catch (Exception e) {
            log.error(String.format("Cannot execute SQL statement `%s`.%nError:%n%s", query, e.getMessage()),e);
            String errorMessage = "Cannot execute SQL statement `%s`.%nError:%n%s";
            if(e.getMessage().contains("No suitable driver")) errorMessage="Cannot execute SQL statement `%s`.%nError:%n%s%n%s";
            throw new RuntimeException(String.format(errorMessage, query, e.getMessage(), "Please download and copy the JDBC driver into $NEO4J_HOME/plugins,more details at https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_load_jdbc_resources"), e);
        }
    }

    static void closeIt(Log log, AutoCloseable...closeables) {
        for (AutoCloseable c : closeables) {
            try {
                if (c!=null) {
                    c.close();
                }
            } catch (Exception e) {
                log.warn(String.format("Error closing %s: %s", c.getClass().getSimpleName(), c),e);
                // ignore
            }
        }
    }

    private static class ResultSetIterator implements Iterator<Map<String, Object>> {
        private final Log log;
        private final ResultSet rs;
        private final String[] columns;
        private final boolean closeConnection;
        private Map<String, Object> map;
        private LoadJdbcConfig config;


        public ResultSetIterator(Log log, ResultSet rs, boolean closeConnection, LoadJdbcConfig config) throws SQLException {
            this.config = config;
            this.log = log;
            this.rs = rs;
            this.columns = getMetaData(rs);
            this.closeConnection = closeConnection;
            this.map = get();
        }

        private String[] getMetaData(ResultSet rs) throws SQLException {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            String[] columns = new String[cols + 1];
            for (int col = 1; col <= cols; col++) {
                columns[col] = meta.getColumnLabel(col);
            }
            return columns;
        }

        @Override
        public boolean hasNext() {
            return this.map != null;
        }

        @Override
        public Map<String, Object> next() {
            Map<String, Object> current = this.map;
            this.map = get();
            return current;
        }

        public Map<String, Object> get() {
            try {
                if (handleEndOfResults()) return null;
                Map<String, Object> row = new LinkedHashMap<>(columns.length);
                for (int col = 1; col < columns.length; col++) {
                    row.put(columns[col], convert(rs.getObject(col), rs.getMetaData().getColumnType(col)));
                }
                return row;
            } catch (Exception e) {
                log.error(String.format("Cannot execute read result-set.%nError:%n%s", e.getMessage()),e);
                closeRs();
                throw new RuntimeException("Cannot execute read result-set.", e);
            }
        }

        private Object convert(Object value, int sqlType) {
            if (value == null) return null;
            if (value instanceof UUID || value instanceof BigInteger || value instanceof BigDecimal) {
                return value.toString();
            }
            ZoneId zoneId = config.getZoneId();
            if (value instanceof LocalDateTime localDateTime) {
                if (zoneId != null) {
                    return localDateTime.atZone(zoneId)
                            .withZoneSameInstant(ZoneId.systemDefault())
                            .toLocalDateTime();
                }
                return value;
            }
            if (Types.TIME == sqlType) {
                return ((java.sql.Time)value).toLocalTime();
            }
            if (Types.TIME_WITH_TIMEZONE == sqlType) {
                return OffsetTime.parse(value.toString());
            }
            if (Types.TIMESTAMP == sqlType) {
                if (zoneId != null) {
                    return ((java.sql.Timestamp)value).toInstant()
                            .atZone(zoneId)
                            .toOffsetDateTime();
                } else {
                    return ((java.sql.Timestamp)value).toLocalDateTime();
                }
            }
            if (Types.TIMESTAMP_WITH_TIMEZONE == sqlType) {
                if (zoneId != null) {
                    return ((java.sql.Timestamp)value).toInstant()
                            .atZone(zoneId)
                            .toOffsetDateTime();
                } else {
                    return OffsetDateTime.parse(value.toString());
                }
            }
            if (Types.DATE == sqlType) {
                return ((java.sql.Date)value).toLocalDate();
            }

            if (Types.ARRAY == sqlType) {
                try {
                    return ((java.sql.Array)value).getArray();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return value;
        }

        private boolean handleEndOfResults() throws SQLException {
            Boolean closed = isRsClosed();
            if (closed!=null && closed) {
                return true;
            }
            if (!rs.next()) {
                closeRs();
                return true;
            }
            return false;
        }

        private void closeRs() {
            Boolean closed = isRsClosed();
            if (closed==null || !closed) {
                closeIt(log, ignore(rs::getStatement), closeConnection ? ignore(()->rs.getStatement().getConnection()) : null);
            }
        }

        private Boolean isRsClosed() {
            try {
                return ignore(rs::isClosed);
            } catch(AbstractMethodError ame) {
                return null;
            }
        }

    }
    interface FailingSupplier<T> {
        T get() throws Exception;
    }
    public static <T> T ignore(FailingSupplier<T> fun) {
        try {
            return fun.get();
        } catch(Exception e) {
            /*ignore*/
        }
        return null;
    }
}
