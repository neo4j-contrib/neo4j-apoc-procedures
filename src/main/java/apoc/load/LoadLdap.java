package apoc.load;

import apoc.ApocConfiguration;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.SearchCursor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.Value;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapURLEncodingException;
import org.apache.directory.api.ldap.model.message.*;
import org.apache.directory.api.ldap.model.message.controls.PagedResults;
import org.apache.directory.api.ldap.model.message.controls.PagedResultsImpl;
import org.apache.directory.api.ldap.model.schema.AttributeType;
import org.apache.directory.api.ldap.model.url.LdapUrl;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LoadLdap {
    @Context
    public Log log;
    @Procedure(name = "apoc.load.ldap", mode = Mode.READ)
    @Description("apoc.load.ldap(\"key\" or {connectionMap}) Load entries from an ldap source (yield entry)")
    public Stream<LDAPResult> ldapQuery(@Name("connection") final Object conn) throws LdapURLEncodingException {

        LDAPManager mgr = new LDAPManager(getConnectionMap(conn), log);

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
        private final Log log;

        private String loginDN;
        private String password;
        private LdapConnection lc;
        private LdapUrl ldapUrl;

        public LDAPManager(Map<String, String> connParms, Log log)  throws LdapURLEncodingException {
            this.ldapUrl = new LdapUrl(connParms.get(LDAP_URL));
            this.loginDN = (null != connParms.get(LDAP_LOGIN_DN_P)) ? connParms.get(LDAP_LOGIN_DN_P) : "";
            this.password = (null != connParms.get(LDAP_LOGIN_PW_P)) ? connParms.get(LDAP_LOGIN_PW_P) : "";
            this.log = log;
        }

        public Stream<LDAPResult> executeSearch() {
            try {
                Iterator<Map<String, Object>> supplier = new EntryListIterator(doPagedSearch().iterator(), this.ldapUrl.getAttributes(), log);
                Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
                return StreamSupport.stream(spliterator, false).map(LDAPResult::new).onClose(() -> closeIt(lc));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        /*
         *  @param pagedSearchControl Optional paginated results with cookie
         *  @return Serach parameters as defined by the LDAP URL with paging controls
         *  @throws LdapException Something blew up
         */
        private SearchRequest buildSearch(PagedResults pagedSearchControl) throws LdapException {
            if (log.isDebugEnabled()) log.debug("Generating new SearchRequest");
            SearchRequest req = new SearchRequestImpl();
            req.setScope(this.ldapUrl.getScope());
            req.setSizeLimit(0);
            req.setFilter(this.ldapUrl.getFilter());
            for (String a : this.ldapUrl.getAttributes()) {
                req.addAttributes(a);
            }
            req.setBase(this.ldapUrl.getDn());
            req.followReferrals();

            if (null != pagedSearchControl) {
                req.addControl(pagedSearchControl);
            } else {
                PagedResults temp = new PagedResultsImpl();
                temp.setSize(100);
                req.addControl(temp);
            }
            if (log.isDebugEnabled()) log.debug(String.format("Generated SearchRequest: %s", req.toString()));
            return req;
        }

        public Collection<Entry> doPagedSearch() {
            if (log.isDebugEnabled()) log.debug("Beginning paged LDAP search");
            List<Entry> allEntries = new ArrayList<>();
            try {
                lc = getConnection();
                boolean hasMoreResults = true;
                while (hasMoreResults) {
                    PagedResults pr = new PagedResultsImpl();
                    pr.setSize(100);
                    SearchRequest req = buildSearch(pr);

                    try (SearchCursor searchCursor = lc.search(req)) {
                        while (searchCursor.next()) {
                            Response resp = searchCursor.get();
                            if (resp instanceof SearchResultEntry) {
                                Entry resultEntry = ((SearchResultEntry) resp).getEntry();
                                allEntries.add(resultEntry);
                            }
                        }
                        SearchResultDone done = searchCursor.getSearchResultDone();
                        PagedResults hasPaged = (PagedResults) done.getControl("1.2.840.113556.1.4.319");
                        if ( (null != hasPaged) && (hasPaged.getCookie().length > 0) ) {
                            if (log.isDebugEnabled()) log.debug("Iterating over the next LDAP search page");
                            pr.setCookie(hasPaged.getCookie());
                            hasMoreResults = true;
                        } else {
                            hasMoreResults = false;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            if (log.isDebugEnabled()) log.debug(String.format("Finished paged LDAP search: %d entries", allEntries.size()));
            return allEntries;
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

    private static class EntryListIterator implements Iterator<Map<String, Object>> {
        private final Iterator<Entry> entries;
        private final List<String> attributes;
        private final Log log;
        private Map<String, Object> map;

        public EntryListIterator(Iterator<Entry> entries, List<String> attributes, Log log) {
            this.entries = entries;
            this.attributes = attributes;
            this.log = log;
            this.map = get();
        }

        @Override
        public boolean hasNext() {
            return this.map != null;
        }

        @Override
        public Map<String, Object> next() {
            Map<String, Object> current = this.map;
            this.map = get();
            return current;
        }

        public Map<String, Object> get() {
            if (this.log.isDebugEnabled()) this.log.debug("Fetching next LDAP entry");
            if (handleEndOfResults()) return null;
            Map<String, Object> entryMap = new LinkedHashMap<>(attributes.size()+1);
            Entry ldapEntry = entries.next();
            if (this.log.isDebugEnabled()) log.debug(String.format("Processing entry: %s", ldapEntry.toString()));
            entryMap.put("dn", ldapEntry.getDn().toString());
            for (Attribute attribute : ldapEntry) {
                String attrName = attribute.getId();
                entryMap.put(attrName, readValue(attribute));
            }
            if (this.log.isDebugEnabled()) log.debug(String.format("entryMap: ", entryMap));
            return entryMap;
        }

        private boolean handleEndOfResults() {
            return !entries.hasNext();
        }

        private Object readValue(Attribute att) {
            if (att == null) return null;
            AttributeType attributeType = att.getAttributeType();
            if (this.log.isDebugEnabled()) this.log.debug(String.format("Processing attribute: %s", att.toString()));
            Object ret;
            if (attributeType.isSingleValued()) {
                if (this.log.isDebugEnabled()) log.debug(String.format("Attribute %s is single valued: %s", att.getId(), att.get().toString()));
                ret = att.get().toString();
            } else {
                List<String> vals = new ArrayList<>();
                if (this.log.isDebugEnabled()) this.log.debug(String.format("Attribute %s is multivalued", att.getId()));
                for (Value val : att) {
                    if (this.log.isDebugEnabled()) this.log.debug(String.format("Attribute %s: %s", att.getId(), val.getNormalized()));
                    vals.add(val.getNormalized());
                }
                ret = vals;
            }
            return ret;
        }
    }

    private static class SearchResultsIterator implements Iterator<Map<String, Object>> {
        private final SearchCursor lsr;
        private final List<String> attributes;
        private Map<String,Object> map;
        public SearchResultsIterator(SearchCursor lsr, List<String> attributes, Log log) {
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
