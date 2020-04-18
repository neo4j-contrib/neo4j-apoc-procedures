package apoc.load.util;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.ldap.model.exception.LdapURLEncodingException;
import org.apache.directory.api.ldap.model.url.LdapUrl;

import java.util.Collections;
import java.util.Map;

public class LoadLdapConfig {
    private final Credentials credentials;
    private int pageSize;
    private final LdapUrl ldapUrl;

    public LoadLdapConfig(Map<String, Object> config, String url) {
        config = (null != config) ? config : Collections.emptyMap();
        this.credentials = new Credentials(
                (String) config.getOrDefault("username", StringUtils.EMPTY),
                (String) config.getOrDefault("password", StringUtils.EMPTY)
        );
        try {
            this.ldapUrl = new LdapUrl(url);
        } catch (LdapURLEncodingException e) {
            throw new RuntimeException(e);
        }
        
        try {
            this.pageSize = (config.containsKey("pageSize")) ? Integer.parseInt((String) config.get("pageSize")) : 100;
        } catch (java.lang.NumberFormatException e) {
            this.pageSize = 100;
        }
    }

    public LoadLdapConfig(Map<String, Object> config) {
        this(config, (String) config.get("url"));
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageSize() {
        return this.pageSize;
    }

    public LdapUrl getLdapUrl() {
        return this.ldapUrl;
    }

    public Credentials getCredentials() {
        return this.credentials;
    }

    public static class Credentials {
        private final String bindDn;
        private final String bindPassword;

        public Credentials(String bindDn, String bindPassword) {
            this.bindDn = bindDn;
            this.bindPassword = bindPassword;
        }

        public String getBindDn() {
            return bindDn;
        }

        public String getBindPassword() {
            return bindPassword;
        }
    }

    public boolean hasCredentials() {
        return this.credentials != null;
    }
}
