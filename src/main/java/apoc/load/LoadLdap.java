package apoc.load;

import apoc.ApocConfiguration;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.SearchCursor;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapURLEncodingException;
import org.apache.directory.api.ldap.model.message.Response;
import org.apache.directory.api.ldap.model.message.SearchRequest;
import org.apache.directory.api.ldap.model.message.SearchRequestImpl;
import org.apache.directory.api.ldap.model.message.SearchResultEntry;
import org.apache.directory.api.ldap.model.url.LdapUrl;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LoadLdap {

    @Procedure(name = "apoc.load.ldap", mode = Mode.READ)
    @Description("apoc.load.ldap(\"key\" or {connectionMap}) Load entries from an ldap source (yield entry)")
    public Stream<LDAPResult> ldapQuery(@Name("connection") final Object conn) throws LdapURLEncodingException {

        LDAPManager mgr = new LDAPManager(getConnectionMap(conn));

        return mgr.executeSearch();
    }

    public static Map<String, String> getConnectionMap(Object conn) throws LdapURLEncodingException {
        Map<String, String> config = new HashMap<>();
        String loginDN = "";
        String loginPW = "";
        String ldapUrl = "";

        if (conn instanceof String) {
            Object value = ApocConfiguration.get("loadldap").get(conn.toString() + ".config");
            // format <ldapURL>[;<logindn>;<loginpw>
            // format <ldapURL>
            if (value == null)
                throw new RuntimeException("No apoc.loadldap."+conn+".config ldap access configuration specified");

            String[] sConf = ((String) value).split(";");
            if (sConf.length == 3) {
                ldapUrl = sConf[0];
                loginDN = sConf[1];
                loginPW = sConf[2];
            } else {
                ldapUrl = sConf[0];
            }
        } else if (conn instanceof Map) {
            Map tempConn = (Map) conn;
            ldapUrl = (null != tempConn.get("ldapURL")) ? (String) tempConn.get("ldapURL") : "";
            loginDN = (null != tempConn.get("loginDN")) ? (String) tempConn.get("loginDN") : "";
            loginPW = (null != tempConn.get("loginPW")) ? (String) tempConn.get("loginPW") : "";
        }

        if (ldapUrl.equals("")) {
            throw new LdapURLEncodingException("LDAP URL cannot be empty");
        }

        config.put("ldapHost", sConf[0]);
        config.put("loginDN", sConf[1]);
        config.put("loginPW", sConf[2]);
        return config;
    }

    public static class LDAPManager {
        private static final String LDAP_LOGIN_DN_P = "loginDN";
        private static final String LDAP_LOGIN_PW_P = "loginPW";
        private static final String LDAP_URL = "ldapURL";

        private String loginDN;
        private String password;
        private LdapConnection lc;
        private LdapUrl ldapUrl;

        public LDAPManager(Map<String, String> connParms)  throws LdapURLEncodingException {
            this.ldapUrl = new LdapUrl(connParms.get(LDAP_URL));
            this.loginDN = (null != connParms.get(LDAP_LOGIN_DN_P)) ? connParms.get(LDAP_LOGIN_DN_P) : "";
            this.password = (null != connParms.get(LDAP_LOGIN_PW_P)) ? connParms.get(LDAP_LOGIN_PW_P) : "";
        }

        public Stream<LDAPResult> executeSearch() {
            try {
                Iterator<Map<String, Object>> supplier = new SearchResultsIterator(doSearch());
                Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
                return StreamSupport.stream(spliterator, false).map(LDAPResult::new).onClose(() -> closeIt(lc));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public SearchCursor doSearch() {
            // getting an ldap connection
            try {
                lc = getConnection();
                // execute query
                SearchRequest req = new SearchRequestImpl();
                req.setScope(this.ldapUrl.getScope());
                req.setSizeLimit(0);
                req.setFilter(this.ldapUrl.getFilter());
                req.addAttributes(String.valueOf(this.ldapUrl.getAttributes()));
                req.setBase(this.ldapUrl.getDn());

                return lc.search(req);
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

        private LdapConnection getConnection() throws LdapException {
            LdapConnection lc = new LdapNetworkConnection(this.ldapUrl.getHost(), this.ldapUrl.getPort(), this.ldapUrl.getScheme().equals("ldaps"));

            // Start out binding with a DN and password
            if (!this.loginDN.equals("") && !this.password.equals("")) {
                lc.bind(this.loginDN, this.password);
            // Next try an unauthenticated authentication bind
            } else if (!this.loginDN.equals("")) {
                lc.bind(this.loginDN);
            // Lastly, bind anonymously
            } else {
                lc.anonymousBind();
            }
            return lc;
        }

        private void op(String s) {
            System.out.println("LDAPManager:>" + s);
        }

    }
    private static class SearchResultsIterator implements Iterator<Map<String, Object>> {
        private final SearchCursor lsr;
        private final List<String> attributes;
        private Map<String,Object> map;
        public SearchResultsIterator(SearchCursor lsr, List<String> attributes) {
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
                Response resp = lsr.get();
                if (resp instanceof SearchResultEntry) {
                    Entry en = ((SearchResultEntry) resp).getEntry();
                    entry.put("dn", en.getDn());
                    if (attributes != null && attributes.size() > 0) {
                        for (int col = 0; col < attributes.size(); col++) {
                            Object val = readValue(en.getAttributes().get(attributes.get(col)));
                            if (val != null) entry.put(attributes.get(col), val);
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
                }

            } catch (CursorException e) {
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
