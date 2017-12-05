package apoc.data.url;

import java.net.URL;
import java.net.MalformedURLException;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

/**
 * This class is pretty simple.  It just constructs a java.net.URL instance
 * from the user's input to do validation/parsing, and delegates the actual
 * functionality to that class.  As such, the behavior of these functions
 * matches that class, which is nice.
 */
public class ExtractURL {
    /**
     * In the context of a user function, we want to ignore parse errors.
     */
    protected static URL uriOrNull(String value) {
        if (value == null) return null;
        try {
            URL u = new URL(value);
            return u;
        } catch(MalformedURLException exc) {
            return null;
        }
    }

    @UserFunction
    @Description("apoc.data.url.protocol('url') YIELD protocol - extract protocol from a URL")
    public String protocol(final @Name("url") String value) {
        URL u = uriOrNull(value);
        return u != null ? u.getProtocol() : null;
    }

    @UserFunction
    @Description("apoc.data.url.port('url') YIELD port - extract a port from a URL; returns -1 if none specified")
    public Long port(final @Name("url") String value) {
        URL u = uriOrNull(value);

        // -1 is the default behavior because thats what java.net.URL does when a port
        // isn't specified.  E.g. the port for http://google.com/ in this context is -1,
        // *not* 80, it's not doing any special protocol inference magic.
        // The port for null is similarly -1.
        return u != null ? new Long(u.getPort()) : -1L;
    }

    @UserFunction
    @Description("apoc.data.url.query('url') YIELD query - extract a query from a URL")
    public String query(final @Name("url") String value) {
        URL u = uriOrNull(value);

        return u != null ? u.getQuery() : null;
    }

    @UserFunction
    @Description("apoc.data.url.path('url') YIELD path - extract a path from a URL")
    public String path(final @Name("url") String value) {
        URL u = uriOrNull(value);
        return u != null ? u.getPath() : null;
    }

    @UserFunction
    @Description("apoc.data.url.file('url') YIELD file - extract a file from a URL")
    public String file(final @Name("url") String value) {
        URL u = uriOrNull(value);
        return u != null ? u.getFile() : null;
    }

    @UserFunction
    @Description("apoc.data.url.anchor('url') YIELD anchor - extract a file from a URL")
    public String anchor(final @Name("url") String value) {
        URL u = uriOrNull(value);
        return u != null ? u.getRef() : null;
    }

    @UserFunction
    @Description("apoc.data.url.userInfo('url') YIELD anchor - extract user information from a URL")
    public String userInfo(final @Name("url") String value) {
        URL u = uriOrNull(value);
        return u != null ? u.getUserInfo() : null;
    }

    @UserFunction
    @Description("apoc.data.url.host('url') YIELD domain - extract the host name from a uri. If nothing was found, yield null.")
    public String host(final @Name("url") String value) {
        URL u = uriOrNull(value);
        return u != null ? u.getHost() : null;
    }

}
