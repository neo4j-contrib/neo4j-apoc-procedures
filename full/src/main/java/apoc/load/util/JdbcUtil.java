/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.load.util;

import apoc.util.Util;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.DatabaseConnectionSources;
import us.fatehi.utility.datasource.MultiUseUserCredentials;


public class JdbcUtil {

    private static final String KEY_NOT_FOUND_MESSAGE = "No apoc.jdbc.%s.url url specified";
    private static final String LOAD_TYPE = "jdbc";

    private JdbcUtil() {}

    public static DatabaseConnectionSource getConnection(String jdbcUrl, LoadJdbcConfig config) throws Exception {
        if (config.hasCredentials()) {
            return createConnection(
                    jdbcUrl,
                    config.getCredentials().getUser(),
                    config.getCredentials().getPassword());
        } else {
            URI uri = new URI(jdbcUrl.substring("jdbc:".length()));
            String userInfo = uri.getUserInfo();
            if (userInfo != null) {
                String cleanUrl =
                        jdbcUrl.substring(0, jdbcUrl.indexOf("://") + 3) + jdbcUrl.substring(jdbcUrl.indexOf("@") + 1);
                String[] user = userInfo.split(":");
                return createConnection(cleanUrl, user[0], user[1]);
            }
            return DatabaseConnectionSources.newDatabaseConnectionSource(jdbcUrl, new MultiUseUserCredentials());
        }
    }

    private static DatabaseConnectionSource createConnection(String jdbcUrl, String userName, String password)
            throws Exception {
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
                return Subject.doAs(subject, (PrivilegedExceptionAction<DatabaseConnectionSource>)
                        () -> DatabaseConnectionSources.newDatabaseConnectionSource(
                                jdbcUrl, new MultiUseUserCredentials(userName, password)));
            } catch (PrivilegedActionException pae) {
                throw pae.getException();
            }
        } else {
            return DatabaseConnectionSources.newDatabaseConnectionSource(
                    jdbcUrl, new MultiUseUserCredentials(userName, password));
        }
    }

    public static String getUrlOrKey(String urlOrKey) {
        return urlOrKey.contains(":")
                ? urlOrKey
                : Util.getLoadUrlByConfigFile(LOAD_TYPE, urlOrKey, "url")
                        .orElseThrow(() -> new RuntimeException(String.format(KEY_NOT_FOUND_MESSAGE, urlOrKey)));
    }

    public static String getSqlOrKey(String sqlOrKey) {
        return sqlOrKey.contains(" ")
                ? sqlOrKey
                : Util.getLoadUrlByConfigFile(LOAD_TYPE, sqlOrKey, "sql").orElse("SELECT * FROM " + sqlOrKey);
    }
}
