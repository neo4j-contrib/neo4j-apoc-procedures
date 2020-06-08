package apoc.load;

import apoc.Extended;
import com.novell.ldap.*;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.ApocConfig.apocConfig;

@Extended
public class LoadLdap {

    @Procedure(name = "apoc.load.ldap", mode = Mode.READ)
    @Description("apoc.load.ldap(\"key\" or {connectionMap},{searchMap}) Load entries from an ldap source (yield entry)")
    public Stream<LDAPResult> ldapQuery(@Name("connection") final Object conn, @Name("search") final Map<String,Object> search) {

        LDAPManager mgr = new LDAPManager(getConnectionMap(conn));

        return mgr.executeSearch(search);
    }

    public static Map<String, Object> getConnectionMap(Object conn) {
        if (conn instanceof String) {
            //String value = "ldap.forumsys.com cn=read-only-admin,dc=example,dc=com password";
            String value = apocConfig().getString("apoc.loadldap" + conn.toString() + ".config");
            // format <ldaphost:port> <logindn> <loginpw>
            if (value == null) throw new RuntimeException("No apoc.loadldap."+conn+".config ldap access configuration specified");
            Map<String, Object> config = new HashMap<>();
            String[] sConf = value.split(" ");
            config.put("ldapHost", sConf[0]);
            config.put("loginDN", sConf[1]);
            config.put("loginPW", sConf[2]);

            return config;

        } else {
            return (Map<String,Object> ) conn;
        }
    }

    public static class LDAPManager {
        private static final String LDAP_HOST_P = "ldapHost";
        private static final String LDAP_LOGIN_DN_P = "loginDN";
        private static final String LDAP_LOGIN_PW_P = "loginPW";
        private static final String SEARCH_BASE_P = "searchBase";
        private static final String SEARCH_SCOPE_P = "searchScope";
        private static final String SEARCH_FILTER_P = "searchFilter";
        private static final String SEARCH_ATTRIBUTES_P = "attributes";

        private static final String SCOPE_BASE = "SCOPE_BASE";
        private static final String SCOPE_ONE = "SCOPE_ONE";
        private static final String SCOPE_SUB = "SCOPE_SUB";

        private int ldapPort;
        private int ldapVersion = LDAPConnection.LDAP_V3;
        private String ldapHost;
        private String loginDN;
        private String password;
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
        }

        public Stream<LDAPResult> executeSearch(Map<String, Object> search) {
            try {
                Iterator<Map<String, Object>> supplier = new SearchResultsIterator(doSearch(search), attributeList);
                Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
                return StreamSupport.stream(spliterator, false).map(LDAPResult::new).onClose(() -> closeIt(lc));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public LDAPSearchResults doSearch(Map<String, Object> search) {
            // parse search parameters
            String searchBase = (String) search.get(SEARCH_BASE_P);
            String searchFilter = (String) search.get(SEARCH_FILTER_P);
            String sScope = (String) search.get(SEARCH_SCOPE_P);
            attributeList = (List<String>) search.get(SEARCH_ATTRIBUTES_P);
            if (attributeList == null) attributeList = new ArrayList<>();
            int searchScope = LDAPConnection.SCOPE_SUB;
            if (sScope.equals(SCOPE_BASE)) {
                searchScope = LDAPConnection.SCOPE_BASE;
            } else if (sScope.equals(SCOPE_ONE)) {
                searchScope = LDAPConnection.SCOPE_ONE;
            } else if (sScope.equals(SCOPE_SUB)) {
                searchScope = LDAPConnection.SCOPE_SUB;
            } else {
                throw new RuntimeException("Invalid scope:" + sScope + ". value scopes are SCOPE_BASE, SCOPE_ONE and SCOPE_SUB");
            }
            // getting an ldap connection
            try {
                lc = getConnection();
                // execute query
                LDAPSearchConstraints cons = new LDAPSearchConstraints();
                cons.setMaxResults(0); // no limit
                LDAPSearchResults searchResults = null;
                if (attributeList == null || attributeList.size() == 0) {
                    searchResults = lc.search(searchBase, searchScope, searchFilter, null, false, cons);
                } else {
                    searchResults = lc.search(searchBase, searchScope, searchFilter, attributeList.toArray(new String[0]), false, cons);
                }
                return searchResults;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static void closeIt(LDAPConnection lc) {
            try {
                lc.disconnect();
            } catch (Exception e) {
                // ignore
                e.printStackTrace();
            }
        }

        private LDAPConnection getConnection() throws LDAPException, UnsupportedEncodingException {
//        LDAPSocketFactory ssf;
//        Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
            // String path ="C:\\j2sdk1.4.2_09\\jre\\lib\\security\\cacerts";
            //op("the trustStore: " + System.getProperty("javax.net.ssl.trustStore"));
            // System.setProperty("javax.net.ssl.trustStore", path);
//        op(" reading the strustStore: " + System.getProperty("javax.net.ssl.trustStore"));
//        ssf = new LDAPJSSESecureSocketFactory();
//        LDAPConnection.setSocketFactory(ssf);


            LDAPConnection lc = new LDAPConnection();
            lc.connect(ldapHost, ldapPort);

            // bind to the server
            lc.bind(ldapVersion, loginDN, password.getBytes("UTF8"));
            // tbd
            // LDAPConnection pooling here?
            //
            return lc;
        }

    }
    private static class SearchResultsIterator implements Iterator<Map<String, Object>> {
        private final LDAPSearchResults lsr;
        private final List<String> attributes;
        private Map<String,Object> map;
        public SearchResultsIterator(LDAPSearchResults lsr, List<String> attributes) {
            this.lsr = lsr;
            this.attributes = attributes;
            this.map = get();
        }

        @Override
        public boolean hasNext() {
            return this.map != null;
        }

        @Override
        public Map<String, Object> next() {
            Map<String,Object> current = this.map;
            this.map = get();
            return current;
        }

        public Map<String, Object> get() {
            if (handleEndOfResults()) return null;
            try {
                Map<String, Object> entry = new LinkedHashMap<>(attributes.size() + 1);
                LDAPEntry en = null;
                en = lsr.next();
                entry.put("dn", en.getDN());
                if (attributes != null && attributes.size() > 0) {
                    for (int col = 0; col < attributes.size(); col++) {
                        Object val = readValue(en.getAttributeSet().getAttribute(attributes.get(col)));
                        if (val != null) entry.put(attributes.get(col),val );
                    }
                } else {
                    // make it dynamic
                    Iterator<LDAPAttribute> iter = en.getAttributeSet().iterator();
                    while (iter.hasNext()) {
                        LDAPAttribute attr = iter.next();
                        Object val = readValue(attr);
                        if (val != null) entry.put(attr.getName(), readValue(attr));
                    }
                }
                //System.out.println("entry " + entry);
                return entry;

            } catch (LDAPException e) {
                e.printStackTrace();
                throw new RuntimeException("Error getting next ldap entry " + e.getLDAPErrorMessage());
            }
        }

        private boolean handleEndOfResults()  {
            if (!lsr.hasMore()) {
                return true;
            }
            return false;
        }
        private Object readValue(LDAPAttribute att) {
            if (att == null) return null;
            if (att.size() == 1) {
                // single value
                // for now everything is string
                return att.getStringValue();
            } else {
                return att.getStringValueArray();
            }
        }
    }

}
