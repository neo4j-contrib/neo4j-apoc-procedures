package apoc.load.util;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.ldap.model.exception.LdapURLEncodingException;
import org.apache.directory.api.ldap.model.url.LdapUrl;

import java.util.Collections;
import java.util.Map;

public class LoadLdapConfig {
    private Credentials credentials;
    private int pageSize;
    private LdapUrl ldapUrl;

    public LoadLdapConfig(Map<String, Object> config, String url) {
        config = (null != config) ? config : Collections.emptyMap();
        // could be prettier
        this.credentials = config.containsKey("credentials") ? createCredentials((Map<String, String>) config.get("credentials")) : new Credentials((String) config.getOrDefault("user", StringUtils.EMPTY), (String) config.getOrDefault("password", StringUtils.EMPTY));
        this.pageSize = (int) config.getOrDefault("pageSize", 100);
        try {
            this.ldapUrl = new LdapUrl(url);
        } catch (LdapURLEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public LoadLdapConfig(Map<String, Object> config) {
        this.credentials = new Credentials(
                (String) config.getOrDefault("user", StringUtils.EMPTY),
                (String) config.getOrDefault("password", StringUtils.EMPTY)
        );
        try {
            this.ldapUrl = new LdapUrl((String) config.getOrDefault("url", LdapUrl.EMPTY_URL));
        } catch (LdapURLEncodingException e) {
            throw new RuntimeException(e);
        }
        this.pageSize = (config.containsKey("pageSize")) ? Integer.parseInt((String) config.get("pageSize")) : 100;
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

    public static LoadLdapConfig.Credentials createCredentials(Map<String,String> credentials) {
        if (!credentials.getOrDefault("user", StringUtils.EMPTY).equals(StringUtils.EMPTY) && !credentials.getOrDefault("password", StringUtils.EMPTY).equals(StringUtils.EMPTY)) {
            return new Credentials(credentials.get("user"), credentials.get("password"));
        } else {
            throw new IllegalArgumentException("In config param credentials must be passed both user and password.");
        }
    }

    public static class Credentials {
        private String bindDn;
        private String bindPassword;

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
