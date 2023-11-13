package apoc.data.email;

import apoc.Extended;
import apoc.util.MissingDependencyException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Map;

import static apoc.data.email.ExtractEmailHandler.extractEmail;

@Extended
public class ExtractEmail {
    public static final String EMAIL_MISSING_DEPS_ERROR = """
            Cannot find the needed jar into the plugins folder in order to use .\s
            Please put the apoc-email-dependencies-5.x.x-all.jar into plugin folder.
            See the documentation: https://neo4j.com/labs/apoc/5/overview/apoc.data/apoc.data.email/#_install_dependencies""";

    @UserFunction("apoc.data.email")
    @Description("apoc.data.email('email_address') as {personal,user,domain} - extract the personal name, user and domain as a map")
    public Map<String,String> email(final @Name("email_address") String value) {
        try {
            return extractEmail(value);
        } catch (NoClassDefFoundError e) {
            throw new MissingDependencyException(EMAIL_MISSING_DEPS_ERROR);
        }
    }
}
