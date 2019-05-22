package apoc.load.util;

import apoc.util.Util;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcUtil {

    private static final String KEY_NOT_FOUND_MESSAGE = "No apoc.jdbc.%s.url url specified";
    private static final String LOAD_TYPE = "jdbc";

    private JdbcUtil() {}

    public static Connection getConnection(String jdbcUrl, LoadJdbcConfig config) throws Exception {
        if(config.hasCredentials()) {
            return createConnection(jdbcUrl, config.getCredentials().getUser(), config.getCredentials().getPassword());
        } else {
            URI uri = new URI(jdbcUrl.substring("jdbc:".length()));
            String userInfo = uri.getUserInfo();
            if (userInfo != null) {
                String cleanUrl = jdbcUrl.substring(0, jdbcUrl.indexOf("://") + 3) + jdbcUrl.substring(jdbcUrl.indexOf("@") + 1);
                String[] user = userInfo.split(":");
                return createConnection(cleanUrl, user[0], user[1]);
            }
            return DriverManager.getConnection(jdbcUrl);
        }
    }

    private static Connection createConnection(String jdbcUrl, String userName, String password) throws Exception {
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
                return Subject.doAs(subject, (PrivilegedExceptionAction<Connection>) () -> DriverManager.getConnection(jdbcUrl, userName, password));
            } catch (PrivilegedActionException pae) {
                throw pae.getException();
            }
        } else {
            return DriverManager.getConnection(jdbcUrl, userName, password);
        }
    }

    public static String getUrlOrKey(String urlOrKey) {
        return urlOrKey.contains(":") ? urlOrKey : Util.getLoadUrlByConfigFile(LOAD_TYPE, urlOrKey, "url").orElseThrow(() -> new RuntimeException(String.format(KEY_NOT_FOUND_MESSAGE, urlOrKey)));
    }

    public static String getSqlOrKey(String sqlOrKey) {
        return sqlOrKey.contains(" ") ? sqlOrKey : Util.getLoadUrlByConfigFile(LOAD_TYPE, sqlOrKey, "sql").orElse("SELECT * FROM " + sqlOrKey);
    }
}
