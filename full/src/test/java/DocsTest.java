import apoc.util.TestUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.query.FunctionInformation;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertFalse;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class DocsTest {

    public static final String GENERATED_DOCUMENTATION_DIR = "../docs/asciidoc/modules/ROOT/examples/generated-documentation";
    public static final String GENERATED_PARTIALS_DOCUMENTATION_DIR = "../docs/asciidoc/modules/ROOT/partials/generated-documentation";
    public static final String GENERATED_OVERVIEW_DIR = "../docs/asciidoc/modules/ROOT/pages/overview";
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
        new File(GENERATED_PARTIALS_DOCUMENTATION_DIR).mkdirs();
        new File(GENERATED_OVERVIEW_DIR).mkdirs();
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
        Map<String, String> docs = createDocsMapping();

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

        Map<String, List<Row>> topLevelNamespaces = rows.stream().filter(value -> value.name.split("\\.").length == 3).collect(Collectors.groupingBy(value -> {
            String[] parts = value.name.split("\\.");
            parts = Arrays.copyOf(parts, parts.length - 1);
            return String.join(".", parts);
        }));

        try (Writer overviewWriter = new OutputStreamWriter(new FileOutputStream(new File(GENERATED_PARTIALS_DOCUMENTATION_DIR, "documentation.adoc")), StandardCharsets.UTF_8);
             Writer navWriter = new OutputStreamWriter(new FileOutputStream(new File(GENERATED_PARTIALS_DOCUMENTATION_DIR, "nav.adoc")), StandardCharsets.UTF_8)) {
            topLevelNamespaces.keySet().stream().sorted().forEach(topLevelNamespace -> {
                new File(GENERATED_OVERVIEW_DIR, topLevelNamespace).mkdirs();

                try (Writer sectionWriter = new OutputStreamWriter(new FileOutputStream(new File(new File(GENERATED_OVERVIEW_DIR, topLevelNamespace), "index.adoc")), StandardCharsets.UTF_8)) {
                    sectionWriter.write("////\nThis file is generated by DocsTest, so don't change it!\n////\n\n");
                    sectionWriter.write("= " + topLevelNamespace + "\n");
                    sectionWriter.write(":description: This section contains reference documentation for the " + topLevelNamespace + " procedures.\n\n");

                    sectionWriter.write(header());

                    for (Row row : topLevelNamespaces.get(topLevelNamespace)) {
                        String releaseType = extended.contains(row.name) ? "full" : "core";
                        sectionWriter.write(String.format("|%s|%s|%s\n",
                                String.format("xref::%s[%s icon:book[]]\n\n%s", "overview/" + topLevelNamespace + "/" + row.name + ".adoc", row.name, row.description.replace("|", "\\|")),
                                String.format("label:%s[]\n", row.type),
                                String.format("label:apoc-%s[]\n", releaseType)));
                    }

                    sectionWriter.write(footer());

                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }

                try {
                    if (topLevelNamespaces.get(topLevelNamespace).size() < 3) {
                        overviewWriter.write("[discrete]\n");
                    }

                    overviewWriter.write("== xref::overview/" + topLevelNamespace + "/index.adoc[]\n\n");
                    overviewWriter.write(header());

                    navWriter.write("** xref::overview/" + topLevelNamespace + "/index.adoc[]\n");
                    for (Row row : topLevelNamespaces.get(topLevelNamespace)) {
                        String releaseType = extended.contains(row.name) ? "full" : "core";
                        overviewWriter.write(String.format("|%s|%s|%s\n",
                                String.format("%s[%s icon:book[]]\n\n%s", "xref::overview/" + topLevelNamespace + "/" + row.name + ".adoc", row.name, row.description.replace("|", "\\|")),
                                String.format("label:%s[]\n", row.type),
                                String.format("label:apoc-%s[]\n", releaseType)));
                        navWriter.write("*** xref::overview/" + topLevelNamespace + "/" + row.name  + ".adoc[]\n");
                    }

                    overviewWriter.write(footer());


                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getMessage(), e );
        }

        DependencyResolver resolver = db.getDependencyResolver();
        GlobalProcedures globalProcedures = resolver.resolveDependency(GlobalProcedures.class);
        Set<ProcedureSignature> allProcedures = globalProcedures.getAllProcedures().stream().filter(signature -> signature.name().toString().startsWith("apoc")).collect(Collectors.toSet());

        for (ProcedureSignature procedure : allProcedures) {
            String[] parts = procedure.name().toString().split("\\.");
            String topLevelDirectory = String.format("%s.%s", parts[0], parts[1]);
            new File(GENERATED_OVERVIEW_DIR, topLevelDirectory).mkdirs();

            try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(new File(GENERATED_OVERVIEW_DIR, topLevelDirectory), procedure.name().toString() + ".adoc")), StandardCharsets.UTF_8)) {
                writer.write("////\nThis file is generated by DocsTest, so don't change it!\n////\n\n");
                writer.write("= " + procedure.name().toString() + "\n");
                writer.write(":description: This section contains reference documentation for the " + procedure.name().toString() + " procedure.\n\n");

                String release = extended.contains(procedure.name().toString()) ? "full" : "core";

                writer.write("label:procedure[] label:apoc-" + release + "[]\n\n");
                writer.write("[.emphasis]\n" + procedure.description().orElse("") + "\n\n");

                writer.write("== Signature\n\n");
                writer.write("[source]\n----\n" + procedure.toString() + "\n----\n\n");

                if(procedure.inputSignature().size() > 0) {

                    writer.write("== Input parameters\n");
                    writer.write("[.procedures, opts=header]\n" +
                            "|===\n" +
                            "| Name | Type | Default \n");

                    for (FieldSignature fieldSignature : procedure.inputSignature()) {
                        writer.write(String.format("|%s|%s|%s\n", fieldSignature.name(), fieldSignature.neo4jType().toString(), fieldSignature.defaultValue().map(DefaultParameterValue::value).orElse(null)));
                    }
                    writer.write("|===\n\n");
                }

                if(procedure.outputSignature().size() > 0) {
                    writer.write("== Output parameters\n");
                    writer.write("[.procedures, opts=header]\n" +
                            "|===\n" +
                            "| Name | Type \n");

                    for (FieldSignature fieldSignature : procedure.outputSignature()) {
                        writer.write(String.format("|%s|%s\n", fieldSignature.name(), fieldSignature.neo4jType().toString()));
                    }
                    writer.write("|===\n\n");
                }

                Optional<String> documentation = docs.keySet().stream()
                        .filter((key) -> Pattern.compile(key).matcher(procedure.name().toString()).matches())
                        .map(value -> String.format("xref::%s", docs.get(value)))
                        .findFirst();
                if(documentation.isPresent()) {
                    writer.write(documentation.get() + "[More documentation of "  + procedure.name().toString() + ",role=more information]\n\n");
                }

            }
            catch ( Exception e )
            {
                throw new RuntimeException( e.getMessage(), e );
            }
        }

        Stream<UserFunctionSignature> loadedFunctions = resolver.resolveDependency( GlobalProcedures.class ).getAllNonAggregatingFunctions().filter(signature -> signature.name().toString().startsWith("apoc"));
        Stream<UserFunctionSignature> loadedAggregationFunctions = resolver.resolveDependency( GlobalProcedures.class ).getAllAggregatingFunctions().filter(signature -> signature.name().toString().startsWith("apoc"));

        Stream.concat(loadedFunctions, loadedAggregationFunctions).forEach(userFunctionSignature -> {
            String[] parts = userFunctionSignature.name().toString().split("\\.");
            String topLevelDirectory = String.format("%s.%s", parts[0], parts[1]);
            new File(GENERATED_OVERVIEW_DIR, topLevelDirectory).mkdirs();

            try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(new File(GENERATED_OVERVIEW_DIR, topLevelDirectory), userFunctionSignature.name().toString() + ".adoc")), StandardCharsets.UTF_8)) {
                writer.write("////\nThis file is generated by DocsTest, so don't change it!\n////\n\n");
                writer.write("= " + userFunctionSignature.name().toString() + "\n");
                writer.write(":description: This section contains reference documentation for the " + userFunctionSignature.name().toString() + " function.\n\n");

                String release = extended.contains(userFunctionSignature.name().toString()) ? "full" : "core";

                writer.write("label:function[] label:apoc-" + release + "[]\n\n");

                writer.write("[.emphasis]\n" + userFunctionSignature.description().orElse("") + "\n\n");

                writer.write("== Signature\n\n");
                writer.write("[source]\n----\n" + userFunctionSignature.toString() + "\n----\n\n");

                if(userFunctionSignature.inputSignature().size() > 0) {

                    writer.write("== Input parameters\n");
                    writer.write("[.procedures, opts=header]\n" +
                            "|===\n" +
                            "| Name | Type | Default \n");

                    for (FieldSignature fieldSignature : userFunctionSignature.inputSignature()) {
                        writer.write(String.format("|%s|%s|%s\n", fieldSignature.name(), fieldSignature.neo4jType().toString(), fieldSignature.defaultValue().map(DefaultParameterValue::value).orElse(null)));
                    }
                    writer.write("|===\n\n");
                }

                Optional<String> documentation = docs.keySet().stream()
                        .filter((key) -> Pattern.compile(key).matcher(userFunctionSignature.name().toString()).matches())
                        .map(value -> String.format("xref::%s", docs.get(value)))
                        .findFirst();

                if(documentation.isPresent()) {
                    writer.write(documentation.get() + "[More documentation of "  + userFunctionSignature.name().toString() + ",role=more information]\n\n");
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e.getMessage(), e );
            }
        });



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

    @NotNull
    private Map<String, String> createDocsMapping() {
        Map<String, String> docs = new HashMap<>();
        docs.put("apoc.path.expand", "graph-querying/expand-paths.adoc");
        docs.put("apoc.path.expandConfig", "graph-querying/expand-paths-config.adoc");
        docs.put("apoc.path.subgraphNodes", "graph-querying/expand-subgraph-nodes.adoc");
        docs.put("apoc.path.subgraphAll", "graph-querying/expand-subgraph.adoc");
        docs.put("apoc.export.cypher.*", "export/cypher.adoc");
        docs.put("apoc.export.json.*", "export/json.adoc");
        docs.put("apoc.export.csv.*", "export/csv.adoc");
        docs.put("apoc.export.graphml.*", "export/graphml.adoc");
        docs.put("apoc.graph.*", "export/gephi.adoc");
        docs.put("apoc.load.json.*|apoc.import.json", "import/load-json.adoc");
        docs.put("apoc.load.csv", "import/load-csv.adoc");
        docs.put("apoc.import.csv", "import/import-csv.adoc");
        docs.put("apoc.import.graphml", "import/graphml.adoc");
        docs.put("apoc.coll.*", "data-structures/collection-list-functions.adoc");
        docs.put("apoc.convert.*", "data-structures/conversion-functions.adoc");
        docs.put("apoc.map.*", "data-structures/map-functions.adoc");
        docs.put("apoc.create.v.*|apoc.create.virtual.*", "virtual/virtual-nodes-rels.adoc");
        docs.put("apoc.math.*|apoc.number.romanToArabic|apoc.number.arabicToRoman", "mathematical/math-functions.adoc");
        docs.put("apoc.meta.*", "database-introspection/meta.adoc");
        docs.put("apoc.nodes.*|apoc.node.*|apoc.any.properties|apoc.any.property|apoc.label.exists", "graph-querying/node-querying.adoc");
        docs.put("apoc.number.format.*|apoc.number.parseInt.*|apoc.number.parseFloat.*", "mathematical/number-conversions.adoc");
        docs.put("apoc.number.exact.*", "mathematical/exact-math-functions.adoc");
        docs.put("apoc.path.*", "graph-querying/path-querying.adoc");
        docs.put("apoc.text.*", "misc/text-functions.adoc");
        docs.put("apoc.util.md5|apoc.util.sha1", "misc/text-functions.adoc#text-functions-hashing");
        docs.put("apoc.mongodb.*", "database-integration/mongodb.adoc");
        docs.put("apoc.nlp.aws.*", "nlp/aws.adoc");
        docs.put("apoc.nlp.gcp.*", "nlp/gcp.adoc");
        docs.put("apoc.nlp.azure.*", "nlp/azure.adoc");
        docs.put("apoc.neighbors.*", "graph-querying/neighborhood-search.adoc");
        docs.put("apoc.monitor.*", "database-introspection/monitoring.adoc");
        docs.put("apoc.periodic.iterate", "graph-updates/periodic-execution.adoc#commit-batching");
        docs.put("apoc.periodic.commit", "graph-updates/periodic-execution.adoc#periodic-commit");
        docs.put("apoc.periodic.rock_n_roll", "graph-updates/periodic-execution.adoc#periodic-rock-n-roll");
        docs.put("apoc.refactor.clone.*", "graph-updates/graph-refactoring/clone-nodes.adoc");
        docs.put("apoc.refactor.cloneSubgraph.*", "graph-updates/graph-refactoring/clone-subgraph.adoc");
        docs.put("apoc.refactor.merge.*", "graph-updates/graph-refactoring/merge-nodes.adoc");
        docs.put("apoc.refactor.to|apoc.refactor.from", "graph-updates/graph-refactoring/redirect-relationship.adoc");
        docs.put("apoc.refactor.invert", "graph-updates/graph-refactoring/invert-relationship.adoc");
        docs.put("apoc.refactor.setType", "graph-updates/graph-refactoring/set-relationship-type.adoc");
        docs.put("apoc.static.*", "misc/static-values.adoc");
        docs.put("apoc.spatial.*", "misc/spatial.adoc");
        docs.put("apoc.schema.*", "indexes/schema-index-operations.adoc");
        docs.put("apoc.search.node.*", "graph-querying/parallel-node-search.adoc");
        docs.put("apoc.trigger.*", "job-management/triggers.adoc");
        docs.put("apoc.ttl.*", "graph-updates/ttl.adoc");
        docs.put("apoc.create.uuid", "graph-updates/uuid.adoc");
        docs.put("apoc.cypher.*", "cypher-execution/index.adoc");
        docs.put("apoc.date.*", "temporal/datetime-conversions.adoc");
        docs.put("apoc.hashing.*", "comparing-graphs/fingerprinting.adoc");
        docs.put("apoc.temporal.*", "temporal/temporal-conversions.adoc");
        docs.put("apoc.uuid.*", "graph-updates/uuid.adoc");
        docs.put("apoc.systemdb.*", "database-introspection/systemdb.adoc");
        docs.put("apoc.periodic.submit|apoc.periodic.schedule|apoc.periodic.list|apoc.periodic.countdown", "job-management/periodic-background.adoc");
        docs.put("apoc.model.jdbc", "database-integration/database-modeling.adoc");
        docs.put("apoc.algo.*", "algorithms/path-finding-procedures.adoc");
        docs.put("apoc.atomic.*", "graph-updates/atomic-updates.adoc");
        docs.put("apoc.bolt.*", "database-integration/bolt-neo4j.adoc");
        docs.put("apoc.case|apoc.do.case|apoc.when|apoc.do.when", "cypher-execution/conditionals.adoc");
        docs.put("apoc.es.*", "database-integration/elasticsearch.adoc");
        docs.put("apoc.refactor.rename.*", "graph-updates/graph-refactoring/rename-label-type-property.adoc");
        docs.put("apoc.couchbase.*", "database-integration/couchbase.adoc");
        docs.put("apoc.create.node.*|apoc.create.*Labels|apoc.create.setP.*|apoc.create.setRel.*|apoc.create.relationship|apoc.nodes.link|apoc.merge.*|apoc.create.remove.*", "graph-updates/data-creation.adoc");
        docs.put("apoc.custom.*", "cypher-execution/cypher-based-procedures-functions.adoc");
        docs.put("apoc.generate.*", "graph-updates/graph-generators.adoc");
        docs.put("apoc.config.*", "database-introspection/config.adoc");
        docs.put("apoc.load.jdbc.*", "database-integration/load-jdbc.adoc");
        docs.put("apoc.load.xml.*|apoc.import.xml|apoc.xml.parse", "import/xml.adoc");
        docs.put("apoc.lock.*", "graph-updates/locking.adoc");
        return docs;
    }

    @NotNull
    private String footer() {
        return "|===\n\n";
    }

    @NotNull
    private String header() {
        return "[.procedures, opts=header, cols='5a,1a,1a']\n" +
                "|===\n" +
                "| Qualified Name | Type | Release\n";
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
