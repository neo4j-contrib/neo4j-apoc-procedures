package apoc.load;

import apoc.ApocConfiguration;
import apoc.load.util.LdapUtil;
import apoc.load.util.LoadLdapConfig;
import org.apache.directory.api.ldap.model.cursor.SearchCursor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.Value;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.message.Response;
import org.apache.directory.api.ldap.model.message.SearchRequest;
import org.apache.directory.api.ldap.model.message.SearchResultDone;
import org.apache.directory.api.ldap.model.message.SearchResultEntry;
import org.apache.directory.api.ldap.model.message.controls.PagedResults;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionPool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.load.util.LdapUtil.*;

public class LoadLdap {
    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    private static final String KEY_NOT_FOUND_MESSAGE = "No apoc.jdbc.%s.url url specified";

    @Procedure(name = "apoc.load.ldap", mode = Mode.READ)
    @apoc.Description("apoc.load.ldap('url', config) YIELD row - run an LDAP query from an LDAP URL")
    public Stream<LDAPResult> ldap(@Name("ldapURL") String url, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return executePagedSearch(url, config);
    }

    @Procedure(name = "apoc.load.ldapfromconfig", mode = Mode.READ)
    @apoc.Description("apoc.load.ldapfromconfig('key') YIELD row - load an LDAP config from config")
    public Stream<LDAPResult> ldapFromConfig(@Name("key") String key) {
        return executePagedSearch(key);
    }

    private Stream<LDAPResult> executePagedSearch(String key) {
        Map<String, Object> apocConfig = ApocConfiguration.get(String.format("%s.%s", LOAD_TYPE, key));
        if (apocConfig.isEmpty()) {
            throw new RuntimeException(String.format("Cannot find configuration with name %s", key));
        }
        return executePagedSearch((String) apocConfig.get("url"), apocConfig);
    }

    private Stream<LDAPResult> executePagedSearch(String url, Map<String, Object> config) {
        List<Entry> allEntries = new ArrayList<>();
        LoadLdapConfig ldapConfig = new LoadLdapConfig(config, url);

        try {
            LdapConnectionPool pool = getConnectionPool(ldapConfig);
            LdapConnection connection = pool.getConnection();
            boolean hasRangeRetrieval = connection.getSupportedControls().contains("1.2.840.113556.1.4.802");
            if (log.isDebugEnabled()) log.debug("Server has ranged retrieval control: " + hasRangeRetrieval);

            if (log.isDebugEnabled()) log.debug("Beginning paged LDAP search");
            SearchRequest req = buildSearch(ldapConfig, null, log);
            boolean hasMoreResults = true;
            while (hasMoreResults) {
                try (SearchCursor searchCursor = connection.search(req)) {
                    while (searchCursor.next()) {
                        Response resp = searchCursor.get();
                        if (resp instanceof SearchResultEntry) {
                            Entry resultEntry = ((SearchResultEntry) resp).getEntry();
                            if (hasRangeRetrieval) {
                                for (Attribute attribute : resultEntry) {
                                    Pattern pattern = Pattern.compile("(\\S+);range=(\\d)-(\\d+)");
                                    Matcher matcher = pattern.matcher(attribute.getId());
                                    if (matcher.find()) {
                                        if (log.isDebugEnabled()) log.debug("Found ranged attribute, iterating over values");
                                        LdapConnection extra = pool.getConnection();
                                        Attribute realAttribute = resultEntry.get(matcher.group(1));
                                        try {
                                            List<Value> combined = rangedRetrievalHandler(resultEntry.getDn(), attribute, extra, log);
                                            for (Value val : combined) {
                                                realAttribute.add(val);
                                            }
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        pool.releaseConnection(extra);
                                        if (log.isDebugEnabled()) log.debug("Found real attribute and updated values");
                                    }
                                }
                            }
                            allEntries.add(resultEntry);
                        }
                    }
                    SearchResultDone done = searchCursor.getSearchResultDone();
                    PagedResults hasPaged = (PagedResults) done.getControl("1.2.840.113556.1.4.319");
                    if ( (null != hasPaged) && (hasPaged.getCookie().length > 0) ) {
                        if (log.isDebugEnabled()) log.debug("Iterating over the next LDAP search page");
                        req = buildSearch(ldapConfig, hasPaged.getCookie(), log);
                        hasMoreResults = true;
                    } else {
                        hasMoreResults = false;
                    }
                }
            }
            pool.releaseConnection(connection);
            if (log.isDebugEnabled()) log.debug(String.format("Finished paged LDAP search: %d entries", allEntries.size()));

            Iterator<Map<String, Object>> supplier = new EntryListIterator(allEntries.iterator(), ldapConfig.getLdapUrl().getAttributes(), log);
            Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
            return StreamSupport.stream(spliterator, false).map(LDAPResult::new).onClose(pool::close);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void closeIt(LdapConnection connection) {
        try {
            connection.unBind();
            connection.close();
        } catch (LdapException | IOException e) {
            log.warn("Error closing LDAP connection");
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
            if (this.log.isDebugEnabled()) this.log.debug(String.format("Processing attribute: %s", att.getId()));

            // Handle uuid attributes separately since they're single valued anyway
            String attrName = att.getId();
            if (attrName.equals("objectguid")) {
                return LdapUtil.getStringFromObjectGuid(att.get().getBytes());
            }
            if (attrName.equals("objectsid")) {
                return LdapUtil.getStringFromObjectSid(att.get().getBytes());
            }

            // Handle datetimes specifically for formatting into Neo4j expectations for datetime
            if (attrName.equalsIgnoreCase("createtimestamp") || attrName.equalsIgnoreCase("modifytimestamp")) {
                return LdapUtil.formatDateTime(att.get().getString());
            }

            if (att.size() == 1) {
                if (this.log.isDebugEnabled()) this.log.debug(String.format("Attribute %s is single value", att.getId()));
                return att.get().getString();
            } else {
                List<String> vals = new ArrayList<>();
                if (this.log.isDebugEnabled()) this.log.debug(String.format("Attribute %s is multivalued", att.getId()));
                for (Value val : att) {
                    if (this.log.isDebugEnabled()) this.log.debug(String.format("Attribute %s: %s", att.getId(), val.getNormalized()));
                    vals.add(val.getNormalized());
                }
                return vals;
            }
        }
    }
}
