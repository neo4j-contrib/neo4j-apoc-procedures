package apoc.util;

import apoc.ApocConfiguration;

/**
 * @author mh
 * @since 29.05.16
 */
public class UrlResolver {
    private final String defaultScheme;
    private final String defaultHost;
    private final int defaultPort;
    private final String defaultUrl;

    public UrlResolver(String defaultScheme, String defaultHost, int defaultPort) {
        this.defaultScheme = defaultScheme;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
        this.defaultUrl = defaultScheme + "://" + defaultHost + ":" + defaultPort;
    }

    public String getUrl(String prefix, String hostOrKey) {
        String url = getConfiguredUrl(prefix, hostOrKey);
        if (url != null) return url;
        url = getConfiguredUrl(prefix, "");
        if (url != null) return url;
        url = resolveHost(hostOrKey);
        return url == null ? defaultUrl : url;
    }

    public String getConfiguredUrl(String prefix, String key) {
        String base = prefix + "." + key + (key == null || key.isEmpty() ? "" : ".");
        String url = ApocConfiguration.get(base + "url", null);
        if (url != null) return url;
        String host = ApocConfiguration.get(base + "host", null);
        return resolveHost(host);
    }

    public String resolveHost(String host) {
        if (host != null) {
            if (host.contains("//")) return host;
            if (host.contains(":")) return defaultScheme + "://" + host;
            return defaultScheme + "://" + host + ":" + defaultPort;
        }
        return null;
    }
}
