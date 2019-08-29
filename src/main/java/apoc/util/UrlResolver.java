package apoc.util;

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
        String url = Util.getLoadUrlByConfigFile(prefix, key, "url")
                .orElse(Util.getLoadUrlByConfigFile(prefix, key, "host")
                        .map(this::resolveHost)
                        .orElse(null));
        return url;
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
