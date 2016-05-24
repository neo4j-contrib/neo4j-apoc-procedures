package apoc.help;

import apoc.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class Help {

    @Procedure("apoc.help")
    @Description("Provides the description of the procedure")
    public Stream<HelpResult> info(@Name("proc") String name) throws Exception {
        boolean searchText = false;

        if (name != null) {
            name = name.trim();
            if (name.endsWith("+")) {
                name = name.substring(0, name.lastIndexOf('+')).trim();
                searchText = true;
            }
        }

        return HelpScanner.find(name, searchText);
    }
}
