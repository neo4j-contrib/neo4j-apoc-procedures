package apoc.data;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.regex.Pattern;

/**
 * Extracts domains from URLs and email addresses
 * @deprecated use ExtractEmail or ExtractURI
 */
@Deprecated
public class Extract {

    public static final Pattern DOMAIN = Pattern.compile("([\\w-]+\\.[\\w-]+)+(\\w+)");

    @UserFunction
    @Description("apoc.data.domain('url_or_email_address') YIELD domain - extract the domain name from a url or an email address. If nothing was found, yield null.")
    public String domain(final @Name("url_or_email_address") String value) {
        if (value != null) {
            if (value.contains("@")) {
                String[] tokens = value.split("[@/<>]");
                for (int i = tokens.length - 1; i >= 0; i--) {
                    String token = tokens[i];
                    if (DOMAIN.matcher(token).matches()) return token;
                }
            } else {
                for (String part : value.split("[@/<>]")) {
                    if (DOMAIN.matcher(part).matches()) return part;
                }
            }
        }
        return null;
    }
}
