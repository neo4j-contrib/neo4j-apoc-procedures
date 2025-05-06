package apoc.load.util;

import org.junit.Test;

import static apoc.load.util.JdbcUtil.obfuscateJdbcUrl;
import static org.junit.Assert.assertEquals;

public class JdbcUtilTest {
    @Test
    public void testObfuscateMysqlJdbcUrl() {
        String input = "jdbc:mysql://myserver:3306/mydb?user=admin&password=secret";
        String expected = "jdbc:mysql://*******";
        assertEquals(expected, obfuscateJdbcUrl(input));
    }

    @Test
    public void testObfuscatePostgresJdbcUrl() {
        String input = "jdbc:postgresql://db.example.com:5432/sales?user=postgres&password=mypass";
        String expected = "jdbc:postgresql://*******";
        assertEquals(expected, obfuscateJdbcUrl(input));
    }

    @Test
    public void testObfuscateOracleJdbcUrl() {
        String input = "jdbc:oracle:thin:scott/tiger@dbhost:1521/ORCL";
        String expected = "jdbc:oracle:thin:*******";
        assertEquals(expected, obfuscateJdbcUrl(input));
    }

    @Test
    public void testObfuscateOracleJdbcUrlWithSpecialChars() {
        String input = "jdbc:oracle:thin:user1/P@ssw0rd@prod-db:1521/serviceName";
        String expected = "jdbc:oracle:thin:*******";
        assertEquals(expected, obfuscateJdbcUrl(input));
    }

    @Test
    public void testObfuscateSqlServerJdbcUrl() {
        String input = "jdbc:sqlserver://dbserver:1433;databaseName=TestDB;user=sa;password=pass";
        String expected = "jdbc:sqlserver://*******";
        assertEquals(expected, obfuscateJdbcUrl(input));
    }

    @Test
    public void testObfuscateJdbcUrlWithNoCredentials() {
        String input = "jdbc:mysql://myserver:3306/mydb";
        String expected = "jdbc:mysql://*******";
        assertEquals(expected, obfuscateJdbcUrl(input));
    }

    @Test
    public void testObfuscateJdbcUrlWithAdditionalParams() {
        String input = "jdbc:mysql://myserver:3306/mydb?ssl=true&connectTimeout=5000";
        String expected = "jdbc:mysql://*******";
        assertEquals(expected, obfuscateJdbcUrl(input));
    }
}
