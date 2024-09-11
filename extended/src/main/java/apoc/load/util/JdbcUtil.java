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
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcUtil {

    private static final String KEY_NOT_FOUND_MESSAGE = "No apoc.jdbc.%s.url url specified";
    private static final String LOAD_TYPE = "jdbc";

    private JdbcUtil() {}


    public static Object getConnection(String jdbcUrl, LoadJdbcConfig config, Class<?> classType) throws Exception {
        if(config.hasCredentials()) {
            return createConnection(jdbcUrl, config.getCredentials().getUser(), config.getCredentials().getPassword(), classType);
        } else {
            URI uri = new URI(jdbcUrl.substring("jdbc:".length()));
            String userInfo = uri.getUserInfo();
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
}
