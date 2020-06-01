package apoc.version;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.UserFunction;

/**
 * @author AgileLARUS
 * @since 24-07-17
 */

public class Version {

    @UserFunction("apoc.version")
    @Description("RETURN apoc.version() | return the current APOC installed version")
    public String version() {
        return Version.class.getPackage().getImplementationVersion();
    }
}
