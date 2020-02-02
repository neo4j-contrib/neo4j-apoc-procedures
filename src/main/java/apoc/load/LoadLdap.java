package apoc.load;

import apoc.ApocConfiguration;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.SearchCursor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.Value;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapURLEncodingException;
import org.apache.directory.api.ldap.model.message.Response;
import org.apache.directory.api.ldap.model.message.SearchRequest;
import org.apache.directory.api.ldap.model.message.SearchRequestImpl;
import org.apache.directory.api.ldap.model.message.SearchResultEntry;
import org.apache.directory.api.ldap.model.schema.AttributeType;
import org.apache.directory.api.ldap.model.url.LdapUrl;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

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

        config.put("ldapURL", ldapUrl);
        config.put("loginDN", loginDN);
        config.put("loginPW", loginPW);
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
                Iterator<Map<String, Object>> supplier = new SearchResultsIterator(doSearch(), this.ldapUrl.getAttributes());
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
                req.addAttributes(String.join(",", this.ldapUrl.getAttributes()));
                req.setBase(this.ldapUrl.getDn());

                return lc.search(req);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public static void closeIt(LdapConnection lc) {
            try {
                lc.unBind();
                lc.close();
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

            // Active Directory won't let you load the schema until bound
            lc.loadSchemaRelaxed();
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
            Map<String, Object> entryMap = new LinkedHashMap<>(attributes.size() + 1);
            /*
                Advance the cursor now primarily so we can detect if this is the last entry
                and the call to available() will return false
             */
            try  {
                this.lsr.next();
            } catch (LdapException | CursorException e) {
                e.printStackTrace();
                throw new RuntimeException("Error requesting next ldap entry " + e.getMessage());
            }
            boolean doneYet = handleEndOfResults();
            if (doneYet && !this.lsr.available()) return null;
            try {
                Response resp = lsr.get();
                if (resp instanceof SearchResultEntry) {
                    Entry ldapEntry = ((SearchResultEntry) resp).getEntry();
                    entryMap.put("dn", ldapEntry.getDn());
                    for (Attribute attribute : ldapEntry) {
                        String attrName = attribute.getId();
                        entryMap.put(attrName, readValue(attribute));
                    }
                }
            } catch (CursorException e) {
                e.printStackTrace();
                throw new RuntimeException("Error getting next ldap entry " + e.getMessage());
            }
            return entryMap;
        }

        private boolean handleEndOfResults()  {
            return this.lsr.isDone();
        }


        /**
         * Attempt to do an analysis on the attribute values and return a sane
         * value. Use the LDAP schema to determine if an attribute is single
         * valued and if it's a String or Boolean. We use the schema rather than the
         * size of the values since a multivalued attribute might indeed have
         * only one value
         * TODO: Better handling of LDAP Integer objects. Numeric strings are off the
         * table because the formatting allows spaces i.e "1 2 3 4"
         * @param att Attribute whose values will be returned
         * @return Probably either an bool, String, or List<Object> or null
         */
        private Object readValue(Attribute att) {
            AttributeType attributeType = att.getAttributeType();
            if (att == null) return null;
            if (attributeType.isSingleValued()) {
                // Are we boolean (LDAP or AD)?
                if ( (attributeType.getSyntaxOid().equals("1.3.6.1.4.1.1466.115.121.1.7")) || (attributeType.getSyntaxOid().equals("2.5.5.8")) ) {
                    return (att.get().toString().equals("TRUE"));
                }
                return att.get().toString();
            } else {
                List<String> vals = new ArrayList<>();
                for (Value val : att) {
                    vals.add(val.toString());
                }
                return vals;
            }
        }
    }
}
