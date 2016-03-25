package apoc.load;

import apoc.result.RowResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 26.02.16
 */
public class Jdbc {

    @Procedure
    public void driver(@Name("driverClass") String driverClass) {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load driver class "+driverClass+" "+e.getMessage());
        }
    }

    @Procedure
    public Stream<RowResult> jdbc(@Name("jdbc") String url, @Name("tableOrSql") String tableOrSelect) {
        String query = tableOrSelect.indexOf(' ') == -1 ?
                "SELECT * FROM " + tableOrSelect : tableOrSelect;
        try {
            Statement stmt = DriverManager.getConnection(url).createStatement();
            ResultSet rs = stmt.executeQuery(query);

            Iterator<Map<String, Object>> supplier = new ResultSetIterator(rs);
            Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
            return StreamSupport.stream(spliterator, false).map(RowResult::new);
        } catch (SQLException e) {
            throw new RuntimeException("Cannot execute SQL statement " + query, e);
        }
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
                    row.put(columns[col], rs.getObject(col));
                }
                return row;
            } catch (SQLException e) {
                throw new RuntimeException("Cannot execute read result-set.", e);
            }
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
