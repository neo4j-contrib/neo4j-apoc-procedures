package apoc.util;


import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;

import java.net.URI;
import java.net.URISyntaxException;

import static apoc.ApocConfig.apocConfig;

/**
 * @author AgileLARUS
 * @since 03.09.17
 */
public class UriResolver {

    private URI uri;
    private String url;
    private String prefix;
    private AuthToken token;

    public UriResolver(String url, String prefix) throws URISyntaxException {
        this.url = url;
        this.prefix = prefix;
    }

    public URI getConfiguredUri() {
        return uri;
    }

    public AuthToken getToken() {
        return token;
    }

    private String getConfiguredUri(String key) {
        String keyUrl = "apoc." + this.prefix + "." + key + ".url";
        if (apocConfig().containsKey("apoc.bolt.url")) {
            key = apocConfig().getString("apoc.bolt.url");
        } else if (apocConfig().containsKey(keyUrl)) {
            key = apocConfig().getString(keyUrl, key);
        }
        return key;
    }

    public void initialize() throws URISyntaxException {
        this.url = getConfiguredUri(this.url);
        URI uri;
        try {
            uri = new URI(this.url);
        } catch (URISyntaxException e) {
            throw new URISyntaxException(e.getInput(), e.getMessage());
        }
        this.uri = uri;
        String[] userInfoArray = uri.getUserInfo() == null ?  new String[2] : uri.getUserInfo().split(":");
        String user = userInfoArray[0];
        String password = userInfoArray[1];
        if(user != null && password == null || user == null && password != null)
            throw new RuntimeException("user and password don't defined check your URL or if you use a key the property in your apoc.conf file");
        this.token = (user != null && password != null) ? AuthTokens.basic(user, password) : AuthTokens.none();
    }
}
