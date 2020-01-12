package apoc.load;

import apoc.ApocConfiguration;
import com.novell.ldap.*;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LoadLdap {

    @Procedure(name = "apoc.load.ldap", mode = Mode.READ)
    @Description("apoc.load.ldap(\"key\" or {connectionMap}) Load entries from an ldap source (yield entry)")
    public Stream<LDAPResult> ldapQuery(@Name("connection") final Object conn) throws MalformedURLException {

        LDAPManager mgr = new LDAPManager(getConnectionMap(conn));

        return mgr.executeSearch();
    }

    public static Map<String, String> getConnectionMap(Object conn) {
        Map<String, String> config = new HashMap<>();
        String loginDN = "";
        String loginPW = "";
        String ldapUrl = "";

        if (conn instanceof String) {
            Object value = ApocConfiguration.get("loadldap").get(conn.toString() + ".config");
            // format <ldapURL>[;<logindn>;<loginpw>]
            // format <ldapURL>
            if (value == null)
                throw new RuntimeException("No apoc.loadldap." + conn + ".config ldap access configuration specified");

            String[] sConf = ((String) value).split(";");

            if (sConf.length == 3) {
                ldapUrl = sConf[0];
                loginDN = sConf[1];
                loginPW = sConf[2];
            } else {
                ldapUrl = sConf[0];
            }
        } else if (conn instanceof Map){
            Map tempConn = (Map) conn;
            ldapUrl = (null != tempConn.get("ldapURL")) ? (String) tempConn.get("ldapURL") : "";
            loginDN = (null != tempConn.get("loginDN")) ? (String) tempConn.get("loginDN") : "";
            loginPW = (null != tempConn.get("loginPW")) ? (String) tempConn.get("loginPW") : "";
        }

        if (ldapUrl.equals("")) {
            throw new RuntimeException("LDAP URL cannot be empty");
        }

        config.put("ldapURL", ldapUrl);
        config.put("loginPW", loginPW);
        config.put("loginDN", loginDN);
        return config;
    }

    public static class LDAPManager {
        private static final String LDAP_LOGIN_DN_P = "loginDN";
        private static final String LDAP_LOGIN_PW_P = "loginPW";
        private static final String LDAP_URL = "ldapURL";

        private int ldapVersion = LDAPConnection.LDAP_V3;
        private String loginDN;
        private String password;
        private LDAPConnection lc;
        private LDAPUrl ldapUrl;

        public LDAPManager(Map<String, String> connParms) throws MalformedURLException {
            this.ldapUrl = new LDAPUrl(connParms.get(LDAP_URL));
            this.loginDN = (null != connParms.get(LDAP_LOGIN_DN_P)) ? connParms.get(LDAP_LOGIN_DN_P) : "";
            this.password = (null != connParms.get(LDAP_LOGIN_PW_P)) ? connParms.get(LDAP_LOGIN_PW_P) : "";
        }

        public Stream<LDAPResult> executeSearch() {
            try {
                Iterator<Map<String, Object>> supplier = new SearchResultsIterator(doSearch(), this.ldapUrl.getAttributeArray());
                Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
                return StreamSupport.stream(spliterator, false).map(LDAPResult::new).onClose(() -> closeIt(lc));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public LDAPSearchResults doSearch() {
            // getting an ldap connection
            try {
                lc = getConnection();
                // execute query
                LDAPSearchConstraints cons = new LDAPSearchConstraints();
                cons.setMaxResults(0); // no limit
                LDAPSearchResults searchResults = null;
                searchResults = lc.search(
                        this.ldapUrl.getDN(),
                        this.ldapUrl.getScope(),
                        this.ldapUrl.getFilter(),
                        this.ldapUrl.getAttributeArray(),
                        false,
                        cons);
                return searchResults;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        private LDAPEntry read(String dn) throws LDAPException, UnsupportedEncodingException {
            if (dn == null) return null;
            LDAPEntry r = null;
            op("read start for dn: " + dn);
            LDAPConnection lc = getConnection();
            r = lc.read(dn);
            closeIt(lc);
            // op( r.toString());
            op("read end");
            return r;
        }

        private LDAPSchema getSchema() throws LDAPException, UnsupportedEncodingException {
            LDAPSchema r = null;
            LDAPConnection lc = getConnection();
            r = lc.fetchSchema(lc.getSchemaDN());
            closeIt(lc);
            //op( r.toString());

            return r;
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
            LDAPConnection lc;
            if (this.ldapUrl.isSecure()) {
                // Use the truststore that Neo4J is using when connecting over TLS
                LDAPSocketFactory ssf = new LDAPJSSESecureSocketFactory();
                lc = new LDAPConnection(ssf);
            } else {
                lc = new LDAPConnection();
            }
            lc.connect(this.ldapUrl.getHost(), this.ldapUrl.getPort());

            // bind to the server
            lc.bind(ldapVersion, loginDN, password.getBytes("UTF8"));
            // tbd
            // LDAPConnection pooling here?
            //
            return lc;
        }

        private void op(String s) {
            System.out.println("LDAPManager:>" + s);
        }

    }
    private static class SearchResultsIterator implements Iterator<Map<String, Object>> {
        private final LDAPSearchResults lsr;
        private final String[] attributes;
        private Map<String,Object> map;
        public SearchResultsIterator(LDAPSearchResults lsr, String[] attributes) {
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
                Map<String, Object> entry = new LinkedHashMap<>(attributes.length + 1);
                LDAPEntry en = null;
                en = lsr.next();
                entry.put("dn", en.getDN());
                if (attributes != null && attributes.length > 0) {
                    for (int col = 0; col < attributes.length; col++) {
                        Object val = readValue(en.getAttributeSet().getAttribute(attributes[col]));
                        if (val != null) entry.put(attributes[col],val );
                    }
                } else {
                    // make it dynamic
                    Iterator iter = en.getAttributeSet().iterator();
                    while (iter.hasNext()) {
                        LDAPAttribute attr = (LDAPAttribute) iter.next();
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
