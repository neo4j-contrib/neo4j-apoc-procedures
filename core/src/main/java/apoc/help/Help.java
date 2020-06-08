package apoc.help;

import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static apoc.util.Util.map;

public class Help {

    @Context
    public Transaction tx;

    private static final Set<String> extended = new HashSet<>();

    public Help() {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("extended.txt")) {
            if (stream != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                String name;
                while ((name = reader.readLine()) != null) {
                    extended.add(name);
                }
            }
        } catch (IOException e) {
            // Failed to load extended file
        }
    }

    @Procedure("apoc.help")
    @Description("Provides descriptions of available procedures. To narrow the results, supply a search string. To also search in the description text, append + to the end of the search string.")
    public Stream<HelpResult> info(@Name("proc") String name) throws Exception {
        boolean searchText = false;
        if (name != null) {
            name = name.trim();
            if (name.endsWith("+")) {
                name = name.substring(0, name.lastIndexOf('+')).trim();
                searchText = true;
            }
        }
        String filter = " WHERE name starts with 'apoc.' " +
                " AND ($name IS NULL  OR toLower(name) CONTAINS toLower($name) " +
                " OR ($desc IS NOT NULL AND toLower(description) CONTAINS toLower($desc))) " +
                "RETURN type, name, description, signature ";

        String query = "WITH 'procedure' as type CALL dbms.procedures() yield name, description, signature " + filter +
                " UNION ALL " +
                "WITH 'function' as type CALL dbms.functions() yield name, description, signature " + filter;
        return tx.execute(query, map("name", name, "desc", searchText ? name : null))
                .stream().map(row -> new HelpResult(row, !extended.contains((String) row.get("name"))));
    }
}
