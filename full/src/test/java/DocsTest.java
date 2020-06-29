import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertFalse;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class DocsTest {

    public static final String GENERATED_DOCUMENTATION_DIR = "../build/generated-documentation";
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.auth_enabled, true)
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, Collections.singletonList("apoc.*"));

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(APOC_UUID_ENABLED, true);

        Set<Class<?>> allClasses = allClasses();
        assertFalse(allClasses.isEmpty());

        for (Class<?> klass : allClasses) {
            if(!klass.getName().endsWith("Test")) {
                TestUtil.registerProcedure(db, klass);
            }
        }

        new File(GENERATED_DOCUMENTATION_DIR).mkdirs();
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

        List<Row> procedureRows = db.executeTransactionally("CALL dbms.procedures() YIELD signature, name, description WHERE name STARTS WITH 'apoc' RETURN 'procedure' AS type, name, description, signature ORDER BY signature", Collections.emptyMap(),
                result -> result.stream().map(record -> new Row(
                        record.get("type").toString(),
                        record.get("name").toString(),
                        record.get("signature").toString(),
                        record.get("description").toString())
                ).collect(Collectors.toList()));
        rows.addAll(procedureRows);

        List<Row> functionRows = db.executeTransactionally("CALL dbms.functions() YIELD signature, name, description WHERE name STARTS WITH 'apoc' RETURN 'function' AS type, name, description, signature ORDER BY signature", Collections.emptyMap(),
                result -> result.stream().map(record -> new Row(
                        record.get("type").toString(),
                        record.get("name").toString(),
                        record.get("signature").toString(),
                        record.get("description").toString())
                ).collect(Collectors.toList()));

        rows.addAll(functionRows);

        Map<String, String> docs = new HashMap<>();
        docs.put("apoc.path.expand", "path-expander-paths");
        docs.put("apoc.path.expandConfig", "path-expander-paths-config");
        docs.put("apoc.path.subgraphNodes", "expand-subgraph-nodes");
        docs.put("apoc.path.subgraphAll", "expand-subgraph");
        docs.put("apoc.export.cypher.*", "export-cypher");
        docs.put("apoc.export.json.*", "export-json");
        docs.put("apoc.export.csv.*", "export-csv");
        docs.put("apoc.export.graphml.*", "graphml-export");
        docs.put("apoc.graph.*", "gephi");
        docs.put("apoc.load.json.*|apoc.import.json", "load-json");
        docs.put("apoc.load.csv", "load-csv");
        docs.put("apoc.import.csv", "import-csv");
        docs.put("apoc.import.graphml", "graphml-import");
        docs.put("apoc.coll.*", "collection-list-functions");
        docs.put("apoc.convert.*", "conversion-functions");
        docs.put("apoc.create.v.*|apoc.create.virtual.*", "virtual-nodes-rels");
        docs.put("apoc.map.*", "map-functions");
        docs.put("apoc.math.*|apoc.number.romanToArabic|apoc.number.arabicToRoman", "math-functions");
        docs.put("apoc.meta.*", "meta-graph");
        docs.put("apoc.nodes.*|apoc.node.*|apoc.any.properties|apoc.any.property|apoc.label.exists", "node-functions");
        docs.put("apoc.number.format.*|apoc.number.parseInt.*|apoc.number.parseFloat.*", "number-conversions");
        docs.put("apoc.number.exact.*", "exact-math-functions");
        docs.put("apoc.path.*", "path-functions");
        docs.put("apoc.text.*", "text-functions");
        docs.put("apoc.util.md5|apoc.util.sha1", "text-functions-hashing");
        docs.put("apoc.mongodb.*", "mongodb");
        docs.put("apoc.nlp.aws.*", "nlp-aws");
        docs.put("apoc.nlp.gcp.*", "nlp-gcp");
        docs.put("apoc.nlp.azure.*", "nlp-azure");
        docs.put("apoc.neighbors.*", "neighbourhood-search");
        docs.put("apoc.monitor.*", "monitoring");
        docs.put("apoc.periodic.iterate", "commit-batching");
        docs.put("apoc.periodic.commit", "periodic-commit");
        docs.put("apoc.periodic.rock_n_roll", "periodic-rock-n-roll");
        docs.put("apoc.refactor.clone.*", "clone-nodes");
        docs.put("apoc.refactor.cloneSubgraph.*", "clone-subgraph");
        docs.put("apoc.refactor.merge.*", "merge-nodes");
        docs.put("apoc.refactor.to|apoc.refactor.from", "redirect-relationship");
        docs.put("apoc.refactor.invert", "invert-relationship");
        docs.put("apoc.refactor.setType", "set-relationship-type");
        docs.put("apoc.static.*", "static-values");
        docs.put("apoc.spatial.*", "spatial");
        docs.put("apoc.schema.*", "schema-index-operations");
        docs.put("apoc.search.node.*", "parallel-node-search");
        docs.put("apoc.trigger.*", "triggers");
        docs.put("apoc.ttl.*", "ttl");
        docs.put("apoc.create.uuid", "auto-uuid");
        docs.put("apoc.cypher.*", "cypher-execution");
        docs.put("apoc.date.*", "datetime-conversions");
        docs.put("apoc.hashing.*", "fingerprinting");
        docs.put("apoc.temporal.*", "temporal-conversions");
        docs.put("apoc.uuid.*", "auto-uuid");
        docs.put("apoc.systemdb.*", "systemdb");
        docs.put("apoc.periodic.submit|apoc.periodic.schedule|apoc.periodic.list|apoc.periodic.countdown", "periodic-background");
        docs.put("apoc.model.jdbc", "database-modeling");

        try (Writer writer = new OutputStreamWriter( new FileOutputStream( new File(GENERATED_DOCUMENTATION_DIR, "documentation.csv")), StandardCharsets.UTF_8 ))
        {
            writer.write("¦type¦qualified name¦signature¦description¦documentation\n");
            for (Row row : rows) {

                Optional<String> documentation = docs.keySet().stream()
                        .filter((key) -> Pattern.compile(key).matcher(row.name).matches())
                        .map(value -> String.format("<<%s>>", docs.get(value)))
                        .findFirst();

                writer.write(String.format("¦%s¦%s¦%s¦%s¦%s\n", row.type, row.name, row.signature, row.description, documentation.orElse("")));
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
            try (Writer writer = new OutputStreamWriter( new FileOutputStream( new File(GENERATED_DOCUMENTATION_DIR, String.format("%s.csv", record.getKey()))), StandardCharsets.UTF_8 ))
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
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(GENERATED_DOCUMENTATION_DIR, String.format("%s.csv", row.name))), StandardCharsets.UTF_8)) {
                writer.write("¦type¦qualified name¦signature¦description\n");

                writer.write(String.format("¦%s¦%s¦%s¦%s\n", row.type, row.name, row.signature, row.description));
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        for (Map.Entry<String, List<Row>> record : collect.entrySet()) {
            try (Writer writer = new OutputStreamWriter( new FileOutputStream( new File(GENERATED_DOCUMENTATION_DIR, String.format("%s-lite.csv", record.getKey()))), StandardCharsets.UTF_8 ))
            {
                writer.write("¦signature\n");
                for (Row row : record.getValue()) {
                    writer.write(String.format("¦%s\n", row.signature));
                }

            }
            catch ( Exception e )
            {
                throw new RuntimeException( e.getMessage(), e );
            }
        }


        for (Row row : rows) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(GENERATED_DOCUMENTATION_DIR, String.format("%s-lite.csv", row.name))), StandardCharsets.UTF_8)) {
                writer.write("¦signature\n");

                writer.write(String.format("¦%s\n",row.signature));
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

    }

    private Set<Class<?>> allClasses() {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .forPackages("apoc")
                .setScanners(new SubTypesScanner(false), new TypeAnnotationsScanner())
                .filterInputsBy(input -> !input.endsWith("Test.class") && !input.endsWith("Result.class") && !input.contains("$"))
        );

        return reflections.getSubTypesOf(Object.class);
    }

}
