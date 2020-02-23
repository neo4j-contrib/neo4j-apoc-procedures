package apoc.load.util;

import apoc.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapURLEncodingException;
import org.apache.directory.api.ldap.model.message.SearchRequest;
import org.apache.directory.api.ldap.model.message.SearchRequestImpl;
import org.apache.directory.api.ldap.model.message.controls.PagedResults;
import org.apache.directory.api.ldap.model.message.controls.PagedResultsImpl;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.url.LdapUrl;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.neo4j.logging.Log;

public class LdapUtil {
    private static final String LOAD_TYPE = "ldap";
    private static final String KEY_NOT_FOUND_MESSAGE = "No apoc.ldap.%s.url url specified";

    public static LdapConnection getConnection(String url, LoadLdapConfig ldapConfig) {
        LdapConnection connection;
        try {
            LdapUrl ldapUrl = new LdapUrl(url);
            boolean isSecure = ldapUrl.getScheme().equals("ldaps://");
            int port = ldapUrl.getPort();
            if (isSecure && (port == -1)) {
                port = LdapConnectionConfig.DEFAULT_LDAPS_PORT;
            } else if (isSecure && !(port == -1)) {
                port = ldapUrl.getPort();
            } else if (!isSecure && (port == -1)) {
                port = LdapConnectionConfig.DEFAULT_LDAP_PORT;
            } else {
                port = ldapUrl.getPort();
            }
            connection = new LdapNetworkConnection(ldapUrl.getHost(), port, isSecure);
            try {
                if (ldapConfig.hasCredentials()) {
                    connection.bind(new Dn(ldapConfig.getCredentials().getBindDn()), ldapConfig.getCredentials().getBindPassword());
                } else {
                    connection.anonymousBind();
                }
            } catch (LdapException e) {
                throw new RuntimeException(e);
            }

        } catch (LdapURLEncodingException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    /*
     *  @param pagedSearchControl Optional paginated results with cookie
     *  @return Serach parameters as defined by the LDAP URL with paging controls
     *  @throws LdapException Something blew up
     */
    public static SearchRequest buildSearch(LoadLdapConfig ldapConfig, byte[] cookie, Log log) throws LdapException {
        if (log.isDebugEnabled()) log.debug("Generating new SearchRequest");
        SearchRequest req = new SearchRequestImpl();
        req.setScope(ldapConfig.getLdapUrl().getScope());
        req.setSizeLimit(0);
        req.setFilter(ldapConfig.getLdapUrl().getFilter());
        for (String a : ldapConfig.getLdapUrl().getAttributes()) {
            req.addAttributes(a);
        }
        req.setBase(ldapConfig.getLdapUrl().getDn());
        req.followReferrals();

        PagedResults pr = new PagedResultsImpl();
        pr.setSize(ldapConfig.getPageSize());
        if (null != cookie) {
            pr.setCookie(cookie);
        }

        req.addControl(pr);
        if (log.isDebugEnabled()) log.debug(String.format("Generated SearchRequest: %s", req.toString()));
        return req;
    }

    public static String getUrlOrKey(String urlOrKey) {
        return urlOrKey.contains(":") ? urlOrKey : Util.getLoadUrlByConfigFile(LOAD_TYPE, urlOrKey, "url").orElseThrow(() -> new RuntimeException(String.format(KEY_NOT_FOUND_MESSAGE, urlOrKey)));
    }

    public static int getPageSize(String urlOrKey) {
        String pageSizeStr = Util.getLoadUrlByConfigFile(LOAD_TYPE, urlOrKey, "pageSize").orElse(StringUtils.EMPTY);
        return (pageSizeStr.equals(StringUtils.EMPTY)) ? 100 : Integer.parseInt(pageSizeStr);
    }
}
