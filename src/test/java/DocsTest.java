import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class DocsTest {

    private GraphDatabaseService db;


    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder()
                .setConfig("apoc.uuid.enabled", "true")
                .setConfig("dbms.security.auth_enabled", "true")
                .newGraphDatabase();

        Set<Class<?>> allClasses = allClasses();
        for (Class<?> klass : allClasses) {
            if(!klass.getName().endsWith("Test") && !klass.getName().endsWith("LifeCycle")) {
                TestUtil.registerProcedure(db, klass);
            }
        }

        new File("build/generated-documentation").mkdirs();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    static class Row {
        private String type;
        private String name;
        private String signature;
        private String description;

        public Row(String type, String name, String signature, String description) {
            this.type = type;
            this.name = name;
            this.signature = signature;
            this.description = description;
        }
    }

    @Test
    public void generateDocs() {
        // given
        List<Row> rows = new ArrayList<>();

        Result proceduresResult = db.execute("CALL dbms.procedures() YIELD signature, name, description WHERE name STARTS WITH 'apoc' RETURN 'procedure' AS type, name, description, signature ORDER BY signature");
        while(proceduresResult.hasNext()) {
            Map<String, Object> record = proceduresResult.next();
            rows.add(new Row(record.get("type").toString(), record.get("name").toString(), record.get("signature").toString(), record.get("description").toString()));
        }

        Result functionsResult = db.execute("CALL dbms.functions() YIELD signature, name, description WHERE name STARTS WITH 'apoc' RETURN 'function' AS type, name, description, signature ORDER BY signature");
        while(functionsResult.hasNext()) {
            Map<String, Object> record = functionsResult.next();
            rows.add(new Row(record.get("type").toString(), record.get("name").toString(), record.get("signature").toString(), record.get("description").toString()));
        }


        try (Writer writer = new OutputStreamWriter( new FileOutputStream( new File("build/generated-documentation/documentation.csv")), StandardCharsets.UTF_8 ))
        {
            writer.write("¦type¦qualified name¦signature¦description\n");
            for (Row row : rows) {
                writer.write(String.format("¦%s¦%s¦%s¦%s\n", row.type, row.name, row.signature, row.description));
            }

        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getMessage(), e );
        }

        Map<String, List<Row>> collect = rows.stream().collect(Collectors.groupingBy(value -> {
            String[] parts = value.name.split("\\.");
            parts = Arrays.copyOf(parts, parts.length - 1);
            return String.join(".", parts);
        }));


        for (Map.Entry<String, List<Row>> record : collect.entrySet()) {
            try (Writer writer = new OutputStreamWriter( new FileOutputStream( new File(String.format("build/generated-documentation/%s.csv", record.getKey()))), StandardCharsets.UTF_8 ))
            {
                writer.write("¦type¦qualified name¦signature¦description\n");
                for (Row row : record.getValue()) {
                    writer.write(String.format("¦%s¦%s¦%s¦%s\n", row.type, row.name, row.signature, row.description));
                }

            }
            catch ( Exception e )
            {
                throw new RuntimeException( e.getMessage(), e );
            }
        }

        for (Row row : rows) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(String.format("build/generated-documentation/%s.csv", row.name))), StandardCharsets.UTF_8)) {
                writer.write("¦type¦qualified name¦signature¦description\n");

                writer.write(String.format("¦%s¦%s¦%s¦%s\n", row.type, row.name, row.signature, row.description));
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    private Set<Class<?>> allClasses() {
        List<ClassLoader> classLoadersList = new LinkedList<>();
        classLoadersList.add(ClasspathHelper.contextClassLoader());
        classLoadersList.add(ClasspathHelper.staticClassLoader());

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(new SubTypesScanner(false /* don't exclude Object.class */), new ResourcesScanner())
                .setUrls(ClasspathHelper.forClassLoader(classLoadersList.toArray(new ClassLoader[0])))
                .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix("apoc"))));


        return reflections.getSubTypesOf(Object.class);
    }

}