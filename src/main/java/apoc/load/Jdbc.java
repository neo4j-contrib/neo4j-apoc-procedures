package apoc.load;

import apoc.result.RowResult;
import apoc.ApocConfiguration;
import apoc.util.MapUtil;
import org.apache.commons.compress.utils.IOUtils;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.math.BigDecimal;
import java.math.BigInteger;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 26.02.16
 */
public class Jdbc {

    static {
        ApocConfiguration.get("jdbc").forEach((k, v) -> {
            if (k.endsWith("driver")) loadDriver(v.toString());
        });
    }

    @Context
    public Log log;

    private static Connection getConnection(String jdbcUrl) throws Exception {
        URI uri = new URI(jdbcUrl.substring("jdbc:".length()));
        String userInfo = uri.getUserInfo();
        String[] user = userInfo != null ? userInfo.split(":"): new String[]{null, ""};
        String cleanUrl = userInfo != null ? jdbcUrl.substring(0,jdbcUrl.indexOf("://")+3)+jdbcUrl.substring(jdbcUrl.indexOf("@")+1) : jdbcUrl;

        return DriverManager.getConnection(cleanUrl, user[0], user[1]);
    }

    @Procedure
    @Description("apoc.load.driver('org.apache.derby.jdbc.EmbeddedDriver') register JDBC driver of source database")
    public void driver(@Name("driverClass") String driverClass) {
        loadDriver(driverClass);
    }

    private static void loadDriver(@Name("driverClass") String driverClass) {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load driver class "+driverClass+" "+e.getMessage());
        }
    }

    @Procedure
    @Description("apoc.load.jdbc('key or url','table or statement') YIELD row - load from relational database, from a full table or a sql statement")
    public Stream<RowResult> jdbc(@Name("jdbc") String urlOrKey, @Name("tableOrSql") String tableOrSelect, @Name
            (value = "params", defaultValue = "[]") List<Object> params) {
        return executeQuery(urlOrKey, tableOrSelect, params.toArray(new Object[params.size()]));
    }

    @Procedure
    @Deprecated
    @Description("deprecated - please use: apoc.load.jdbc('key or url','statement',[params]) YIELD row - load from relational database, from a sql statement with parameters")
    public Stream<RowResult> jdbcParams(@Name("jdbc") String urlOrKey, @Name("sql") String select, @Name("params") List<Object> params) {
        return executeQuery(urlOrKey, select,params.toArray(new Object[params.size()]));
    }

    private Stream<RowResult> executeQuery(String urlOrKey, String tableOrSelect, Object...params) {
        String url = urlOrKey.contains(":") ? urlOrKey : getJdbcUrl(urlOrKey);
        String query = tableOrSelect.indexOf(' ') == -1 ? "SELECT * FROM " + tableOrSelect : tableOrSelect;
        try {
            Connection connection = getConnection(url);
            try {
                PreparedStatement stmt = connection.prepareStatement(query);
                try {
                    for (int i = 0; i < params.length; i++) stmt.setObject(i + 1, params[i]);
                    ResultSet rs = stmt.executeQuery();
                    rs.setFetchSize(5000);
                    Iterator<Map<String, Object>> supplier = new ResultSetIterator(log, rs, true);
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

    @Procedure
    @Description("apoc.load.jdbcUpdate('key or url','statement',[params]) YIELD row - update relational database, from a SQL statement with optional parameters")
    public Stream<RowResult> jdbcUpdate(@Name("jdbc") String urlOrKey, @Name("query") String query, @Name(value = "params", defaultValue = "[]") List<Object> params) {
        log.info( String.format( "Executing SQL update: %s", query ) );
        return executeUpdate(urlOrKey, query, params.toArray(new Object[params.size()]));
    }

    private Stream<RowResult> executeUpdate(String urlOrKey, String query, Object...params) {
        String url = urlOrKey.contains(":") ? urlOrKey : getJdbcUrl(urlOrKey);
        try {
            Connection connection = getConnection(url);
            try {
            PreparedStatement stmt = connection.prepareStatement(query);
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

    private static String getJdbcUrl(String key) {
        Object value = ApocConfiguration.get("jdbc").get(key + ".url");
        if (value == null) throw new RuntimeException("No apoc.jdbc."+key+".url jdbc url specified");
        return value.toString();
    }

    private static class ResultSetIterator implements Iterator<Map<String, Object>> {
        private final Log log;
        private final ResultSet rs;
        private final String[] columns;
        private final boolean closeConnection;
        private Map<String, Object> map;

        public ResultSetIterator(Log log, ResultSet rs, boolean closeConnection) throws SQLException {
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
                    row.put(columns[col], convert(rs.getObject(col)));
                }
                return row;
            } catch (Exception e) {
                log.error(String.format("Cannot execute read result-set.%nError:%n%s", e.getMessage()),e);
                closeRs();
                throw new RuntimeException("Cannot execute read result-set.", e);
            }
        }

        private Object convert(Object value) {
            if (value instanceof UUID || value instanceof BigInteger || value instanceof BigDecimal) {
                return value.toString();
            }
            if (value instanceof java.util.Date) {
                return ((java.util.Date) value).getTime();
            }
            return value;
        }

        private boolean handleEndOfResults() throws SQLException {
            Boolean closed = ignore(rs::isClosed);
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
            Boolean closed = ignore(rs::isClosed);
            if (closed==null || !closed) {
                closeIt(log, ignore(rs::getStatement), closeConnection ? ignore(()->rs.getStatement().getConnection()) : null);
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
