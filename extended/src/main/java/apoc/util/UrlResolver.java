package apoc.util;

/**
 * @author mh
 * @since 29.05.16
 */
public class UrlResolver {
    private final String defaultScheme;
    private final Integer defaultPort;
    private final String defaultUrl;

    public UrlResolver(String defaultScheme, String defaultHost, Integer defaultPort) {
        this.defaultScheme = defaultScheme;
        this.defaultPort = defaultPort;
        String url = defaultScheme + "://" + defaultHost;
        if (defaultPort != null) {
            url += ":" + defaultPort;
        }
        this.defaultUrl = url;
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
