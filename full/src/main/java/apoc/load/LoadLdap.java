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
package apoc.load;

import static apoc.ApocConfig.apocConfig;

import apoc.Extended;
import apoc.util.Util;
import com.novell.ldap.*;
import com.unboundid.ldap.sdk.*;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchScope;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.stream.Stream;
import javax.net.ssl.SSLSocketFactory;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Extended
public class LoadLdap {

    @Context
    public Log log;

    @Procedure(name = "apoc.load.ldap", mode = Mode.READ)
    @Description(
            "apoc.load.ldap(\"key\" or {connectionMap},{searchMap}) Load entries from an ldap source (yield entry)")
    public Stream<LDAPResult> ldapQuery(
            @Name("connection") final Object conn, @Name("search") final Map<String, Object> search) {

        LDAPManager mgr = new LDAPManager(getConnectionMap(conn, log));

        return mgr.executeSearch(search);
    }

    public static Map<String, Object> getConnectionMap(Object conn, Log log) {
        if (conn instanceof String) {
            // String value = "ldap.forumsys.com cn=read-only-admin,dc=example,dc=com password";
            String key = String.format("apoc.loadldap.%s.config", conn);
            String value = apocConfig().getString(key);
            // format <ldaphost:port> <logindn> <loginpw>
            if (value == null) {
                // fallback: if `apoc.loadldap.<LDAP_KEY>.config` is not set
                // we check for a config with key `apoc.loadldap<LDAP_KEY>.config`
                String keyOld = String.format("apoc.loadldap%s.config", conn);
                value = apocConfig().getString(keyOld);

                // if the value is set and log == null (that is, not from the test LoadLdapTest.testLoadLDAPConfig),
                // we print a log warn, since the correct way should be with a dot before <LDAP_KEY>
                if (value != null && log != null) {
                    String msgWarn = "Not to cause breaking-change, the current config `%s` is valid,\n"
                            + "but in future releases it will be removed in favor of `%s` (with dot before `%s`),\n"
                            + "as documented here: https://neo4j.com/labs/apoc/5/database-integration/load-ldap/#_credentials.\n";
                    String msgWarnFormatted = String.format(msgWarn, keyOld, key, conn);
                    log.warn(msgWarnFormatted);
                }
            }

            // if neither `apoc.loadldap.<LDAP_KEY>.config` nor `apoc.loadldap<LDAP_KEY>.config` is set.
            // we throw an error
            if (value == null) {
                throw new RuntimeException("No " + key + " ldap access configuration specified");
            }
            Map<String, Object> config = new HashMap<>();
            String[] sConf = value.split(" ");
            config.put("ldapHost", sConf[0]);
            config.put("loginDN", sConf[1]);
            config.put("loginPW", sConf[2]);

            return config;

        } else {
            return (Map<String, Object>) conn;
        }
    }

    public static class LDAPManager {
        private static final String LDAP_HOST_P = "ldapHost";
        private static final String LDAP_LOGIN_DN_P = "loginDN";
        private static final String LDAP_LOGIN_PW_P = "loginPW";
        private static final String LDAP_SSL = "ssl";
        private static final String SEARCH_BASE_P = "searchBase";
        private static final String SEARCH_SCOPE_P = "searchScope";
        private static final String SEARCH_FILTER_P = "searchFilter";
        private static final String SEARCH_ATTRIBUTES_P = "attributes";

        private static final String SCOPE_BASE = "SCOPE_BASE";
        private static final String SCOPE_ONE = "SCOPE_ONE";
        private static final String SCOPE_SUB = "SCOPE_SUB";

        private int ldapPort;
        private String ldapHost;
        private String loginDN;
        private String password;
        private boolean ssl;
        private LDAPConnection lc;
        private List<String> attributeList;

        public LDAPManager(Map<String, Object> connParms) {

            String sLdapHostPort = (String) connParms.get(LDAP_HOST_P);
            if (sLdapHostPort.indexOf(":") > -1) {
                this.ldapHost = sLdapHostPort.substring(0, sLdapHostPort.indexOf(":"));
                this.ldapPort = Integer.parseInt(sLdapHostPort.substring(sLdapHostPort.indexOf(":") + 1));
            } else {
                this.ldapHost = sLdapHostPort;
                this.ldapPort = 389; // default
            }

            this.loginDN = (String) connParms.get(LDAP_LOGIN_DN_P);
            this.password = (String) connParms.get(LDAP_LOGIN_PW_P);
            this.ssl = Util.toBoolean(connParms.get(LDAP_SSL));
        }

        public Stream<LDAPResult> executeSearch(Map<String, Object> search) {
            try {
                return doSearch(search).getSearchEntries().stream()
                        .map(i -> getMapFromEntry(i, attributeList))
                        .map(LDAPResult::new)
                        .onClose(() -> closeIt(lc));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private Map<String, Object> getMapFromEntry(SearchResultEntry entry, List<String> attributes) {
            Map<String, Object> map = new LinkedHashMap<>(attributes.size() + 1);
            map.put("dn", entry.getDN());

            if (attributes.isEmpty()) {
                entry.getAttributes().forEach(i -> {
                    Object value = readValue(i);
                    map.put(i.getName(), value);
                });
            } else {
                for (String attribute : attributes) {
                    Object value = readValue(entry.getAttribute(attribute));
                    if (value != null) map.put(attribute, value);
                }
            }

            return map;
        }

        private Object readValue(Attribute att) {
            if (att == null) return null;
            if (att.size() == 1) {
                // single value
                // for now everything is string
                return att.getValue();
            } else {
                return att.getValues();
            }
        }

        public SearchResult doSearch(Map<String, Object> search) {
            // parse search parameters
            String searchBase = (String) search.get(SEARCH_BASE_P);
            String searchFilter = (String) search.getOrDefault(SEARCH_FILTER_P, "(objectClass=*)");
            String sScope = (String) search.get(SEARCH_SCOPE_P);
            attributeList = (List<String>) search.get(SEARCH_ATTRIBUTES_P);
            if (attributeList == null) attributeList = new ArrayList<>();

            int searchScope =
                    switch (sScope) {
                        case SCOPE_BASE -> SearchScope.BASE_INT_VALUE;
                        case SCOPE_ONE -> SearchScope.ONE_INT_VALUE;
                        case SCOPE_SUB -> SearchScope.SUB_INT_VALUE;
                        default -> throw new RuntimeException(
                                "Invalid scope:" + sScope + ". value scopes are SCOPE_BASE, SCOPE_ONE and SCOPE_SUB");
                    };
            // getting an ldap connection
            try {
                lc = getConnection();
                // execute query
                SearchResult searchResults;
                SearchScope scope = SearchScope.valueOf(searchScope);
                if (attributeList.isEmpty()) {
                    searchResults = lc.search(searchBase, scope, searchFilter);
                } else {
                    searchResults = lc.search(searchBase, scope, searchFilter, attributeList.toArray(new String[0]));
                }
                return searchResults;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static void closeIt(LDAPConnection lc) {
            try {
                lc.close();
            } catch (Exception e) {
                // ignore
            }
        }

        private LDAPConnection getConnection() throws GeneralSecurityException, LDAPException {

            SSLSocketFactory socketFactory = getSocketFactory();
            lc = new LDAPConnection(socketFactory);

            lc.connect(ldapHost, ldapPort);
            lc.bind(loginDN, password);

            return lc;
        }

        private SSLSocketFactory getSocketFactory() throws GeneralSecurityException {
            if (ssl || ldapPort == 636) {
                SSLUtil sslUtil = new SSLUtil(new TrustAllTrustManager());
                return sslUtil.createSSLSocketFactory();
            } else {
                return null;
            }
        }
    }
}
