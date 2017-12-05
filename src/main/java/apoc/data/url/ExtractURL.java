package apoc.data.url;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import static apoc.util.Util.map;

/**
 * This class is pretty simple.  It just constructs a java.net.URL instance
 * from the user's input to do validation/parsing, and delegates the actual
 * functionality to that class.  As such, the behavior of these functions
 * matches that class, which is nice.
 */
public class ExtractURL {
    @UserFunction("apoc.data.url")
    @Description("apoc.data.url('url') as {protocol,host,port,path,query,file,anchor,user} | turn URL into map structure")
    public Map<String, Object> parse(final @Name("url") String value) {
        if (value == null) return null;
        try {
            URL u = new URL(value);
            Long port = u.getPort() == -1 ? null : (long) u.getPort();
            return map("protocol", u.getProtocol(), "user", u.getUserInfo(), "host", u.getHost(), "port", port, "path", u.getPath(),"file", u.getFile(), "query", u.getQuery(), "anchor", u.getRef());
        } catch (MalformedURLException exc) {
            return null;
        }
    }
}
