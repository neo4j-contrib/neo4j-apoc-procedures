package apoc.help;

import apoc.Extended;
import apoc.periodic.PeriodicExtended;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.io.*;
import java.net.URL;
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
        File extendedFile = new File("src/main/resources/extended.txt");
        FileOutputStream fos = new FileOutputStream(extendedFile);

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos))) {
            db.executeTransactionally("CALL dbms.procedures() YIELD name WHERE name STARTS WITH 'apoc' AND name <> 'apoc.help' RETURN name", Collections.emptyMap(),
                    result -> {
                        result.stream().forEach(record -> {
                            try {
                                bw.write(record.get("name").toString());
                                bw.newLine();
                            } catch (IOException ignored) {
                            }
                        });
                        return null;
                    });

            db.executeTransactionally("CALL dbms.functions() YIELD name WHERE name STARTS WITH 'apoc'  AND name <> 'apoc.help' RETURN name", Collections.emptyMap(),
                    result -> {
                        result.stream().forEach(record -> {
                            try {
                                bw.write(record.get("name").toString());
                                bw.newLine();
                            } catch (IOException ignored) {
                            }
                        });
                        return null;
                    });
        }

        TestUtil.testCall(db, "CALL apoc.help($text)", map("text", "rock_n_roll_while"), (row) -> {
            assertEquals(false, row.get("core"));
        });
    }

}
