package apoc.help;

import apoc.Extended;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import org.junit.jupiter.api.AfterAll;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.util.UtilExtended.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author mh
 * @since 06.11.16
 */
public class HelpExtendedTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Help.class);

        Set<Class<?>> allClasses = extendedClasses();
        assertFalse(allClasses.isEmpty());

        for (Class<?> klass : allClasses) {
            if (!klass.getName().endsWith("Test")) {
                TestUtil.registerProcedure(db, klass);
            }
        }
    }

    @AfterAll
    public void tearDown() {
        db.shutdown();
    }

    private Set<Class<?>> extendedClasses() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .forPackages("apoc")
                .setScanners(new SubTypesScanner(false), new TypeAnnotationsScanner())
                .filterInputsBy(input -> !input.endsWith("Test.class") && !input.endsWith("Result.class") && !input.contains("$"))
        );

        return reflections.getTypesAnnotatedWith(Extended.class);
    }

    @Test
    public void indicateNotCore() {
        List<String> expected = listHelpExtendedProcsAndFuncs();
        List<String> actual = listCypherCommandsProcsAndFuncs();
        assertEquals(expected, actual);

        TestUtil.testCall(db, "CALL apoc.help($text)", map("text", "expireIn"), (row) -> {
            assertEquals(false, row.get("core"));
        });
    }
    
    private List<String> listHelpExtendedProcsAndFuncs() {
        String query = "CALL apoc.help('') YIELD name, core WHERE core = false RETURN name";
        List<String> helpExtendedProcedures = getNames(query);
        Collections.sort(helpExtendedProcedures);
        return helpExtendedProcedures;
    }

    private List<String> listCypherCommandsProcsAndFuncs() {
        String commonReturnStatement = " YIELD name WHERE name STARTS WITH 'apoc' AND name <> 'apoc.help' RETURN name";
        List<String> procsAndFuncs = getNames("SHOW FUNCTIONS" + commonReturnStatement);
        List<String> procedures = getNames("SHOW PROCEDURES" + commonReturnStatement);

        procsAndFuncs.addAll(procedures);
        Collections.sort(procsAndFuncs);
        return procsAndFuncs;
    }

    private List<String> getNames(String query) {
        return db.executeTransactionally(query, Collections.emptyMap(),
                result -> result.<String>columnAs("name").stream()
                        .collect(Collectors.toCollection(ArrayList::new)));
    }

}
