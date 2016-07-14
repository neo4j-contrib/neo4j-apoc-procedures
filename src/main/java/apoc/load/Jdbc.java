package apoc.load;

import apoc.Description;
import apoc.result.RowResult;
import apoc.ApocConfiguration;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

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
    public Stream<RowResult> jdbc(@Name("jdbc") String urlOrKey, @Name("tableOrSql") String tableOrSelect) {
        return executeQuery(urlOrKey, tableOrSelect);
    }

    @Procedure
    @Description("apoc.load.jdbcParams('key or url','statement',[params]) YIELD row - load from relational database, from a sql statement with parameters")
    public Stream<RowResult> jdbcParams(@Name("jdbc") String urlOrKey, @Name("sql") String select, @Name("params") List<Object> params) {
        return executeQuery(urlOrKey, select,params.toArray(new Object[params.size()]));
    }

    private Stream<RowResult> executeQuery(@Name("jdbc") String urlOrKey, @Name("tableOrSql") String tableOrSelect, Object...params) {
        String url = urlOrKey.contains(":") ? urlOrKey : getJdbcUrl(urlOrKey);
        String query = tableOrSelect.indexOf(' ') == -1 ? "SELECT * FROM " + tableOrSelect : tableOrSelect;
        try {
            Connection connection = DriverManager.getConnection(url);
            PreparedStatement stmt = connection.prepareStatement(query);
            for (int i = 0; i < params.length; i++) stmt.setObject(i+1, params[i]);
            ResultSet rs = stmt.executeQuery();

            Iterator<Map<String, Object>> supplier = new ResultSetIterator(rs);
            Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
            return StreamSupport.stream(spliterator, false).map(RowResult::new).onClose( () -> closeIt(stmt,connection));
        } catch (SQLException e) {
            log.error(String.format("Cannot execute SQL statement `%s`.%nError:%n%s", query, e.getMessage()),e);
            throw new RuntimeException(String.format("Cannot execute SQL statement `%s`.%nError:%n%s", query, e.getMessage()), e);
        }
    }

    static void closeIt(AutoCloseable...closeables) {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
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
        private final ResultSet rs;
        private final String[] columns;
        private Map<String, Object> map;

        public ResultSetIterator(ResultSet rs) throws SQLException {
            this.rs = rs;
            this.columns = getMetaData(rs);
            this.map = get();
        }

        private String[] getMetaData(ResultSet rs) throws SQLException {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            String[] columns = new String[cols + 1];
            for (int col = 1; col <= cols; col++) {
                columns[col] = meta.getColumnName(col);
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
            } catch (SQLException e) {
                throw new RuntimeException("Cannot execute read result-set.", e);
            }
        }

        private Object convert(Object value) {
            if (value instanceof UUID || value instanceof BigInteger || value instanceof BigDecimal) {
                return value.toString();
            }
            return value;
        }

        private boolean handleEndOfResults() throws SQLException {
            if (rs.isClosed()) {
                return true;
            }
            if (!rs.next()) {
                if (!rs.isClosed()) {
//                    rs.close();
                    rs.getStatement().close();
                }
                return true;
            }
            return false;
        }
    }
}
