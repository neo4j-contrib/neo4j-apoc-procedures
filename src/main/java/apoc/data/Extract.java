package apoc.data;

import org.neo4j.procedure.Description;
import apoc.result.StringResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Extract {

    public static final Pattern DOMAIN = Pattern.compile("([\\w-]+\\.[\\w-]+)+(\\w+)");

    @UserFunction
    @Description("apoc.data.domain('url_or_email_address') YIELD domain - extract the domain name from a url or an email address. If nothing was found, yield null.")
    public String domain(final @Name("url_or_email_address") String value) {
        if (value != null) {
            for (String part : value.split("[@/<>]")) {
                if (DOMAIN.matcher(part).matches()) return part;
            }
        }
        return null;
    }
}
