package apoc.load.util;

import apoc.util.Util;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSources;
import us.fatehi.utility.datasource.MultiUseUserCredentials;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import java.math.BigDecimal;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JdbcUtil {

    public static final Map<Class<?>, String> DUCK_TYPE_MAP = new HashMap<>();
    public static final Map<Class<?>, String> POSTGRES_TYPE_MAP = new HashMap<>();
    public static final Map<Class<?>, String> MYSQL_TYPE_MAP = new HashMap<>();
    public static final String VARCHAR_TYPE = "VARCHAR(1000)";
    
    static {
        // DuckDB mappings
        DUCK_TYPE_MAP.put(String.class, "VARCHAR");
        DUCK_TYPE_MAP.put(Integer.class, "INTEGER");
        DUCK_TYPE_MAP.put(int.class, "INTEGER");
        DUCK_TYPE_MAP.put(Long.class, "BIGINT");
        DUCK_TYPE_MAP.put(long.class, "BIGINT");
        DUCK_TYPE_MAP.put(Double.class, "DOUBLE");
        DUCK_TYPE_MAP.put(double.class, "DOUBLE");
        DUCK_TYPE_MAP.put(Float.class, "FLOAT");
        DUCK_TYPE_MAP.put(float.class, "FLOAT");
        DUCK_TYPE_MAP.put(Boolean.class, "BOOLEAN");
        DUCK_TYPE_MAP.put(boolean.class, "BOOLEAN");
        DUCK_TYPE_MAP.put(BigDecimal.class, "DECIMAL");
        DUCK_TYPE_MAP.put(Date.class, "DATE");
        DUCK_TYPE_MAP.put(Time.class, "TIME");
        DUCK_TYPE_MAP.put(Timestamp.class, "TIMESTAMP");
        DUCK_TYPE_MAP.put(LocalDate.class, "DATE");
        DUCK_TYPE_MAP.put(LocalTime.class, "TIME");
        DUCK_TYPE_MAP.put(OffsetTime.class, "TIME");
        DUCK_TYPE_MAP.put(Instant.class, "DATETIME");
        DUCK_TYPE_MAP.put(LocalDateTime.class, "TIMESTAMP");
        DUCK_TYPE_MAP.put(ZonedDateTime.class, "DATETIME");
        DUCK_TYPE_MAP.put(OffsetDateTime.class, "DATETIME");
        DUCK_TYPE_MAP.put(Duration.class, "INTERVAL");
        DUCK_TYPE_MAP.put(byte[].class, "BLOB");

        // PostgreSQL mappings
        POSTGRES_TYPE_MAP.putAll(DUCK_TYPE_MAP);
        POSTGRES_TYPE_MAP.put(Double.class, "FLOAT");
        POSTGRES_TYPE_MAP.put(double.class, "FLOAT");
        POSTGRES_TYPE_MAP.put(LocalDateTime.class, "DATE");
        POSTGRES_TYPE_MAP.put(ZonedDateTime.class, "DATE");
        POSTGRES_TYPE_MAP.put(OffsetDateTime.class, "DATE");
        POSTGRES_TYPE_MAP.put(String.class, VARCHAR_TYPE);
        POSTGRES_TYPE_MAP.put(byte[].class, "bytea");

        // MySQL mappings
        MYSQL_TYPE_MAP.putAll(DUCK_TYPE_MAP);
        MYSQL_TYPE_MAP.put(String.class, VARCHAR_TYPE);
        MYSQL_TYPE_MAP.put(LocalDateTime.class, "DATETIME");
        MYSQL_TYPE_MAP.put(Duration.class, VARCHAR_TYPE);
    }

    public static final String KEY_NOT_FOUND_MESSAGE = "No apoc.jdbc.%s.url url specified";
    private static final String LOAD_TYPE = "jdbc";

    private JdbcUtil() {}

    public static String toSqlCompatibleDateTime(ZonedDateTime zonedDateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();

        return localDateTime.format(formatter);
    }

    public static String toSqlCompatibleTimeFormat(OffsetTime zonedDateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        LocalTime localTime = zonedDateTime.toLocalTime();

        return localTime.format(formatter);
    }

    public static Object getConnection(String jdbcUrl, LoadJdbcConfig config, Class<?> classType) throws Exception {
        if(config.hasCredentials()) {
            return createConnection(jdbcUrl, config.getCredentials().getUser(), config.getCredentials().getPassword(), classType);
        } else {
            String userInfo = null;
            try {
                URI uri = new URI(jdbcUrl.substring("jdbc:".length()));
                userInfo = uri.getUserInfo();
            } catch (Exception e) {
                // with DuckDB we can pass a jdbc url like "jdbc:duckdb:"
                // this will fail executing new URI(..) due to `java.net.URISyntaxException: Expected scheme-specific part at index 7: duckdb:`
            }
            if (userInfo != null) {
                String cleanUrl = jdbcUrl.substring(0, jdbcUrl.indexOf("://") + 3) + jdbcUrl.substring(jdbcUrl.indexOf("@") + 1);
                String[] user = userInfo.split(":");
                return createConnection(cleanUrl, user[0], user[1], classType);
            }
            return DriverManager.getConnection(jdbcUrl);
        }
    }

    private static Object createConnection(String jdbcUrl, String userName, String password, Class<?> classType) throws Exception {
        if (jdbcUrl.contains(";auth=kerberos")) {
            String client = System.getProperty("java.security.auth.login.config.client", "KerberosClient");
            LoginContext lc = new LoginContext(client, callbacks -> {
                for (Callback cb : callbacks) {
                    if (cb instanceof NameCallback) ((NameCallback) cb).setName(userName);
                    if (cb instanceof PasswordCallback) ((PasswordCallback) cb).setPassword(password.toCharArray());
                }
            });
            lc.login();
            Subject subject = lc.getSubject();
            try {
                return Subject.doAs(subject, (PrivilegedExceptionAction<?>) () -> createConnectionByClass(jdbcUrl, userName, password, classType));
            } catch (PrivilegedActionException pae) {
                throw pae.getException();
            }
        } else {
            return createConnectionByClass(jdbcUrl, userName, password, classType);
        }
    }

    /**
     * We return `DatabaseConnectionSources` for Model.java, 
     * as SchemaCrawlerUtility.getCatalog accepts only `DatabaseConnectionSource` class,
     * otherwise we return a `Connection`, via `DriverManager.getConnection`, for Jdbc.java,
     * as `DatabaseConnectionSource` causes these error: https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/4141
     */
    private static Object createConnectionByClass(String jdbcUrl, String userName, String password, Class<?> classType) throws SQLException {
        if (classType.isAssignableFrom(DatabaseConnectionSource.class)) {
            return DatabaseConnectionSources.newDatabaseConnectionSource(jdbcUrl, new MultiUseUserCredentials(userName, password));
        }
        return DriverManager.getConnection(jdbcUrl, userName, password);
    }


    public static String getUrlOrKey(String urlOrKey) {
        return urlOrKey.contains(":") ? urlOrKey : Util.getLoadUrlByConfigFile(LOAD_TYPE, urlOrKey, "url").orElseThrow(() -> new RuntimeException(String.format(KEY_NOT_FOUND_MESSAGE, urlOrKey)));
    }

    public static String getSqlOrKey(String sqlOrKey) {
        return sqlOrKey.contains(" ") ? sqlOrKey : Util.getLoadUrlByConfigFile(LOAD_TYPE, sqlOrKey, "sql").orElse("SELECT * FROM " + sqlOrKey);
    }

    public static String obfuscateJdbcUrl(String query) {
        return query.replaceAll("(jdbc:[^:]+://)([^\\s\\\"']+)", "$1*******");
    }
}
