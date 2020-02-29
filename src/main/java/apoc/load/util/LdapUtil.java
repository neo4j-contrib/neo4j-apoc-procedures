package apoc.load.util;

import apoc.ApocConfiguration;
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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LdapUtil {
    public static final String LOAD_TYPE = "ldap";
    private static final String KEY_NOT_FOUND_MESSAGE = "No apoc.ldap.%s.url url specified";

    public static LdapConnection getConnection(LoadLdapConfig ldapConfig) {
        LdapConnection connection;
        LdapUrl ldapUrl = ldapConfig.getLdapUrl();
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

    public static LoadLdapConfig getFromConfigFile(String key) {
        Map<String, Object> temp = new HashMap<>();
        temp.put("url", Util.getLoadUrlByConfigFile(LOAD_TYPE, key, "url"));
        temp.put("username", Util.getLoadUrlByConfigFile(LOAD_TYPE, key, "username"));
        temp.put("password", Util.getLoadUrlByConfigFile(LOAD_TYPE, key, "password"));
        temp.put("pageSize", Util.getLoadUrlByConfigFile(LOAD_TYPE, key, "pageSize"));
        return new LoadLdapConfig(temp);
    }

    public static String getUrlOrKey(String urlOrKey) {
        return urlOrKey.contains(":") ? urlOrKey : Util.getLoadUrlByConfigFile(LOAD_TYPE, urlOrKey, "url").orElseThrow(() -> new RuntimeException(String.format(KEY_NOT_FOUND_MESSAGE, urlOrKey)));
    }

    public static int getPageSize(String urlOrKey) {
        String pageSizeStr = Util.getLoadUrlByConfigFile(LOAD_TYPE, urlOrKey, "pageSize").orElse(StringUtils.EMPTY);
        return (pageSizeStr.equals(StringUtils.EMPTY)) ? 100 : Integer.parseInt(pageSizeStr);
    }

    public static String getUsernameOrKey(String usernameOrKey) {
        return (usernameOrKey.equals(StringUtils.EMPTY)) ? Util.getLoadUrlByConfigFile(LOAD_TYPE, usernameOrKey, "binddn").orElse(StringUtils.EMPTY) : usernameOrKey;
    }

    public static String getPasswordOrKey(String passwordOrKey) {
        return (passwordOrKey.equals(StringUtils.EMPTY)) ? Util.getLoadUrlByConfigFile(LOAD_TYPE, passwordOrKey, "password").orElse(StringUtils.EMPTY) : passwordOrKey;
    }

    public static String getUuidFromEntryUuid(byte[] entryUuid) {
        ByteBuffer bb = ByteBuffer.wrap(entryUuid);
        long high = bb.getLong();
        long low = bb.getLong();
        return new UUID(high, low).toString();
    }

    public static String getStringFromObjectGuid(byte[] objectGuid) {
        byte[] rearranged = new byte[16];
        rearranged[0] = objectGuid[3];
        rearranged[1] = objectGuid[2];
        rearranged[2] = objectGuid[1];
        rearranged[3] = objectGuid[0];
        rearranged[4] = objectGuid[5];
        rearranged[5] = objectGuid[4];
        rearranged[6] = objectGuid[7];
        rearranged[7] = objectGuid[6];
        System.arraycopy(rearranged, 8, objectGuid, 8, 8);
        return getUuidFromEntryUuid(rearranged);
    }

    public static String getStringFromObjectSid(byte[] objectSid) {
        StringBuilder strSid = new StringBuilder("S-");

        int revision = objectSid[0];
        strSid.append(revision);

        int countSubAuths = objectSid[1] & 0xFF;

        long authority = 0;
        for (int i=2; i<=7;i++) {
            authority |= ((long) objectSid[i]) << (8 * (5 - (i - 2)));
        }
        strSid.append("-");
        strSid.append(Long.toHexString(authority));

        int offset = 8;
        int size = 4;
        for (int j=0; j<countSubAuths; j++) {
            long subAuthority = 0;
            for (int k=0; k<size; k++) {
                subAuthority |= (long) (objectSid[offset+k] & 0xFF) << (8 * k);
            }
            strSid.append("-");
            strSid.append(subAuthority);
            offset += size;
        }
        return strSid.toString();
    }
}
