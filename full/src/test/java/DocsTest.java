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

import java.io.*;
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

    public static final String GENERATED_DOCUMENTATION_DIR = "../docs/asciidoc/modules/ROOT/examples/generated-documentation";
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
        docs.put("apoc.path.expand", "graph-querying/expand-paths.adoc[]");
        docs.put("apoc.path.expandConfig", "graph-querying/expand-paths-config.adoc[]");
        docs.put("apoc.path.subgraphNodes", "graph-querying/expand-subgraph-nodes.adoc[]");
        docs.put("apoc.path.subgraphAll", "graph-querying/expand-subgraph.adoc[]");
        docs.put("apoc.export.cypher.*", "export/cypher.adoc[]");
        docs.put("apoc.export.json.*", "export/json.adoc[]");
        docs.put("apoc.export.csv.*", "export/csv.adoc[]");
        docs.put("apoc.export.graphml.*", "export/graphml.adoc[]");
        docs.put("apoc.graph.*", "export/gephi.adoc[]");
        docs.put("apoc.load.json.*|apoc.import.json", "import/load-json.adoc[]");
        docs.put("apoc.load.csv", "import/load-csv.adoc[]");
        docs.put("apoc.import.csv", "import/import-csv.adoc[]");
        docs.put("apoc.import.graphml", "import/graphml.adoc[]");
        docs.put("apoc.coll.*", "data-structures/collection-list-functions.adoc[]");
        docs.put("apoc.convert.*", "data-structures/conversion-functions.adoc[]");
        docs.put("apoc.map.*", "data-structures/map-functions.adoc[]");
        docs.put("apoc.create.v.*|apoc.create.virtual.*", "virtual/virtual-nodes-rels.adoc[]");
        docs.put("apoc.math.*|apoc.number.romanToArabic|apoc.number.arabicToRoman", "mathematical/math-functions.adoc[]");
        docs.put("apoc.meta.*", "database-introspection/meta.adoc[]");
        docs.put("apoc.nodes.*|apoc.node.*|apoc.any.properties|apoc.any.property|apoc.label.exists", "graph-querying/node-querying.adoc[]");
        docs.put("apoc.number.format.*|apoc.number.parseInt.*|apoc.number.parseFloat.*", "mathematical/number-conversions.adoc[]");
        docs.put("apoc.number.exact.*", "mathematical/exact-math-functions.adoc[]");
        docs.put("apoc.path.*", "graph-querying/path-querying.adoc[]");
        docs.put("apoc.text.*", "misc/text-functions.adoc[]");
        docs.put("apoc.util.md5|apoc.util.sha1", "misc/text-functions.adoc#text-functions-hashing[Hashing Functions]");
        docs.put("apoc.mongodb.*", "database-integration/mongodb.adoc[]");
        docs.put("apoc.nlp.aws.*", "nlp/aws.adoc[]");
        docs.put("apoc.nlp.gcp.*", "nlp/gcp.adoc[]");
        docs.put("apoc.nlp.azure.*", "nlp/azure.adoc[]");
        docs.put("apoc.neighbors.*", "graph-querying/neighborhood-search.adoc[]");
        docs.put("apoc.monitor.*", "database-introspection/monitoring.adoc[]");
        docs.put("apoc.periodic.iterate", "graph-updates/periodic-execution.adoc#commit-batching[Periodic Iterate]");
        docs.put("apoc.periodic.commit", "graph-updates/periodic-execution.adoc#periodic-commit[Periodic Commit]");
        docs.put("apoc.periodic.rock_n_roll", "graph-updates/periodic-execution.adoc#periodic-rock-n-roll[Periodic Rock 'n' Roll]");
        docs.put("apoc.refactor.clone.*", "graph-updates/graph-refactoring/clone-nodes.adoc[]");
        docs.put("apoc.refactor.cloneSubgraph.*", "graph-updates/graph-refactoring/clone-subgraph.adoc[]");
        docs.put("apoc.refactor.merge.*", "graph-updates/graph-refactoring/merge-nodes.adoc[]");
        docs.put("apoc.refactor.to|apoc.refactor.from", "graph-updates/graph-refactoring/redirect-relationship.adoc[]");
        docs.put("apoc.refactor.invert", "graph-updates/graph-refactoring/invert-relationship.adoc[]");
        docs.put("apoc.refactor.setType", "graph-updates/graph-refactoring/set-relationship-type.adoc[]");
        docs.put("apoc.static.*", "misc/static-values.adoc[]");
        docs.put("apoc.spatial.*", "misc/spatial.adoc[]");
        docs.put("apoc.schema.*", "indexes/schema-index-operations.adoc[]");
        docs.put("apoc.search.node.*", "graph-querying/parallel-node-search.adoc[]");
        docs.put("apoc.trigger.*", "job-management/triggers.adoc[]");
        docs.put("apoc.ttl.*", "graph-updates/ttl.adoc[]");
        docs.put("apoc.create.uuid", "graph-updates/uuid.adoc[]");
        docs.put("apoc.cypher.*", "cypher-execution/index.adoc[]");
        docs.put("apoc.date.*", "temporal/datetime-conversions.adoc[]");
        docs.put("apoc.hashing.*", "comparing-graphs/fingerprinting.adoc[]");
        docs.put("apoc.temporal.*", "temporal/temporal-conversions.adoc[]");
        docs.put("apoc.uuid.*", "graph-updates/uuid.adoc[]");
        docs.put("apoc.systemdb.*", "database-introspection/systemdb.adoc[]");
        docs.put("apoc.periodic.submit|apoc.periodic.schedule|apoc.periodic.list|apoc.periodic.countdown", "job-management/periodic-background.adoc[]");
        docs.put("apoc.model.jdbc", "database-integration/database-modeling.adoc[]");

        Set<String> extended = new HashSet<>();
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

        try (Writer writer = new OutputStreamWriter( new FileOutputStream( new File(GENERATED_DOCUMENTATION_DIR, "documentation.csv")), StandardCharsets.UTF_8 ))
        {
            writer.write("¦type¦qualified name¦signature¦description¦core¦documentation\n");
            for (Row row : rows) {

                Optional<String> documentation = docs.keySet().stream()
                        .filter((key) -> Pattern.compile(key).matcher(row.name).matches())
                        .map(value -> String.format("xref::%s", docs.get(value)))
                        .findFirst();

                writer.write(String.format("¦%s¦%s¦%s¦%s¦%s¦%s\n",
                        row.type,
                        row.name,
                        row.signature,
                        row.description,
                        !extended.contains(row.name),
                        documentation.orElse("")));
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
