package apoc.load;

import org.apache.commons.compress.utils.IOUtils;
import org.neo4j.procedure.Description;
import apoc.result.RowResult;
import apoc.ApocConfiguration;
import apoc.util.MapUtil;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
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
            Connection connection = DriverManager.getConnection(url);
            PreparedStatement stmt = connection.prepareStatement(query);
            for (int i = 0; i < params.length; i++) stmt.setObject(i+1, params[i]);
            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(5000);
            Iterator<Map<String, Object>> supplier = new ResultSetIterator(log, rs,true);
            Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
            return StreamSupport.stream(spliterator, false)
                    .map(RowResult::new)
                    .onClose( () -> closeIt(log, stmt, connection));
        } catch (SQLException e) {
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
            Connection connection = DriverManager.getConnection(url);
            PreparedStatement stmt = connection.prepareStatement(query);
            for (int i = 0; i < params.length; i++) stmt.setObject(i+1, params[i]);
            int updateCount = stmt.executeUpdate();
            closeIt(log, stmt, connection);
            Map<String,Object> result = MapUtil.map( "count", updateCount );
            return Stream.of( result )
                    .map( RowResult::new );
        } catch (SQLException e) {
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
                    int sqlType = rs.getMetaData().getColumnType(col);
                    row.put(columns[col], convert(col, sqlType));
                }
                return row;
            } catch (SQLException e) {
                throw new RuntimeException("Cannot execute read result-set.", e);
            }
        }

        private Object convert(int col,int type) throws SQLException {
            Object value = rs.getObject(col);

            switch (type) {
                case Types.TIMESTAMP:
                    return rs.getTimestamp(col).getTime();
                case Types.DATE:
                    return rs.getTimestamp(col).getTime();
                case Types.OTHER:
                    return value.toString();
                case Types.BIGINT:
                    return value.toString();
                case Types.DECIMAL:
                    return value.toString();
                case Types.BLOB:
                    Blob blob = ((Blob) value);
                    return getValue(blob.getBinaryStream());
                case Types.CLOB:
                    Clob clob = ((Clob) value);
                    return getValue(clob.getAsciiStream());
                case Types.BINARY:
                    byte[] bytes = (byte[]) value;
                    value = new String(bytes);
            }
            return value;
        }

        private Object getValue(InputStream is){
            try {
                return new String(IOUtils.toByteArray(is));
            } catch (Exception e) {
                return "";
            }
            finally {
                try {
                    is.close();
                } catch(Exception e) {
                }
            }
        }

        private boolean handleEndOfResults() throws SQLException {
            if (rs.isClosed()) {
                return true;
            }
            if (!rs.next()) {
                if (!rs.isClosed()) {
                    //                    rs.close();
                    closeIt(log, rs.getStatement(), closeConnection ? rs.getStatement().getConnection() : null);
                }
                return true;
            }
            return false;
        }
    }
}