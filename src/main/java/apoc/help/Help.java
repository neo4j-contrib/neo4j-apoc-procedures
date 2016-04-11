package apoc.help;

import apoc.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class Help {

    @Procedure("apoc.help")
    @Description("Provides the description of the procedure")
    public Stream<HelpResult> info(@Name("proc") String name) throws Exception {
        return HelpScanner.find(name);
    }
}
