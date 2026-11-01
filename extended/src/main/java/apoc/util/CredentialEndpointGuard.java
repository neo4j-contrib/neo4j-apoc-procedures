package apoc.util;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Binds server-held credentials (apoc.conf keys, credentials stored in the system database)
 * to the destinations the server trusts, so that a caller-controlled URL never receives them.
 * Two URLs are considered equivalent when they share the same origin (scheme, host and port).
 */
public class CredentialEndpointGuard {

    private CredentialEndpointGuard() {}

    public static boolean isSameOrigin(String url, String trustedUrl) {
        String origin = origin(url);
        return origin != null && origin.equals(origin(trustedUrl));
    }

    public static boolean isTrusted(String url, Collection<String> trustedUrls) {
        return trustedUrls.stream()
                .filter(Objects::nonNull)
                .anyMatch(trustedUrl -> isSameOrigin(url, trustedUrl));
    }

    /**
     * True if the url is an https one, on the default port, whose host fully matches the pattern
     */
    public static boolean isHttpsHostMatching(String url, Pattern hostPattern) {
        if (StringUtils.isBlank(url)) {
            return false;
        }
        try {
            URI uri = URI.create(url.trim());
            return "https".equalsIgnoreCase(uri.getScheme())
                    && (uri.getPort() == -1 || uri.getPort() == 443)
                    && uri.getHost() != null
                    && hostPattern.matcher(uri.getHost().toLowerCase(Locale.ROOT)).matches();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    static String origin(String url) {
        if (StringUtils.isBlank(url)) {
            return null;
        }
        try {
            URI uri = URI.create(url.trim());
            String scheme = uri.getScheme();
            if (scheme == null) {
                return null;
            }
            scheme = scheme.toLowerCase(Locale.ROOT);
            return scheme + "://" + hostAndPort(uri, scheme);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String hostAndPort(URI uri, String scheme) {
        if (uri.getHost() != null) {
            int port = uri.getPort() == -1 ? defaultPort(scheme) : uri.getPort();
            return uri.getHost().toLowerCase(Locale.ROOT) + ":" + port;
        }
        // registry-based authority, e.g. a host name containing an underscore
        String authority = uri.getRawAuthority();
        return authority == null
                ? ""
                : authority.substring(authority.lastIndexOf('@') + 1).toLowerCase(Locale.ROOT);
    }

    private static int defaultPort(String scheme) {
        return switch (scheme) {
            case "http" -> 80;
            case "https" -> 443;
            default -> -1;
        };
    }
}
