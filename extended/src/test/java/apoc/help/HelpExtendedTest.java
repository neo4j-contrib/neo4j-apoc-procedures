package apoc.help;

import apoc.Extended;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
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

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.util.Util.map;
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
    public void indicateNotCore() throws IOException {
//        File extendedFile = new File("src/main/resources/extended.txt");
//        FileOutputStream fos = new FileOutputStream(extendedFile);

//        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos))) {
        String s = "SHOW PROCEDURES";
        List<String> strings = extracted(s);

        String s1 = "SHOW FUNCTIONS";
        List<String> strings2 = extracted(s1);
//        }

        strings2.addAll(strings);
        Collections.sort(strings2);
        
        String s2 = "CALL apoc.help('') YIELD name, core WHERE core = false RETURN name";
        List<String> stringList = getStrings(s2);

        System.out.println("stringList = " + stringList);
        
        Collections.sort(stringList);
        assertEquals(stringList, strings2);

        TestUtil.testCall(db, "CALL apoc.help($text)", map("text", "expireIn"), (row) -> {
            assertEquals(false, row.get("core"));
        });
    }

    private List<String> extracted(String s1) {
        String query = s1 + " YIELD name WHERE name STARTS WITH 'apoc' AND name <> 'apoc.help' RETURN name";
        return getStrings(query);
    }

    private List<String> getStrings(String query) {
        return db.executeTransactionally(query, Collections.emptyMap(),
                result -> result.<String>columnAs("name").stream()
                        .collect(Collectors.toCollection(ArrayList::new)));
    }

}
