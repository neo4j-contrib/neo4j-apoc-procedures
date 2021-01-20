import org.jetbrains.annotations.NotNull;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.procs.*;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DocumentationGenerator {
    private final List<Row> rows;
    private final GraphDatabaseAPI db;
    private Set<String> extended;
    private Map<String, String> docs;
    private final DependencyResolver resolver;

    class Row {
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

        public String getName() {
            return name;
        }
    }

    DocumentationGenerator(GraphDatabaseAPI db, Set<String> extended, Map<String, String> docs) {
        this.db = db;
        this.extended = extended;
        this.docs = docs;
        rows = new ArrayList<>();
        resolver = db.getDependencyResolver();


        List<Row> procedureRows = collectProcedures();
        List<Row> functionRows = collectFunctions();

        rows.addAll(procedureRows);
        rows.addAll(functionRows);
    }

    public Set<String> getDeprecated() {
        Stream<String> procedures = allProcedures().stream().filter(item -> item.deprecated().isPresent()).map(item -> item.name().toString());
        Stream<String> functions = allFunctions().filter(item -> item.deprecated().isPresent()).map(item -> item.name().toString());
        return Stream.concat(procedures, functions).collect(Collectors.toSet());
    }

    public List<Row> getRows() {
         return rows;
    }

    public void writeAllToCsv(Map<String, String> docs, Set<String> extended) {
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(DocsTest.GENERATED_DOCUMENTATION_DIR, "documentation.csv")), StandardCharsets.UTF_8)) {
            writer.write("¦type¦qualified name¦signature¦description¦core¦documentation\n");
            for (Row row : rows) {

                Optional<String> documentation = docs.keySet().stream()
                        .filter((key) -> Pattern.compile(key).matcher(row.name).matches())
                        .map(value -> String.format("xref::%s", docs.get(value)))
                        .findFirst();
                String description = row.description.replaceAll("(\\{[a-zA-Z0-9_][a-zA-Z0-9_-]+})", "\\\\$1");
                writer.write(String.format("¦%s¦%s¦%s¦%s¦%s¦%s\n",
                        row.type,
                        row.name,
                        row.signature,
                        description,
                        !extended.contains(row.name),
                        documentation.orElse("")));
            }

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void writeNamespaceCsvs() {
        Map<String, List<Row>> namespaces = rows.stream().collect(Collectors.groupingBy(value -> {
            String[] parts = value.name.split("\\.");
            parts = Arrays.copyOf(parts, parts.length - 1);
            return String.join(".", parts);
        }));


        for (Map.Entry<String, List<Row>> record : namespaces.entrySet()) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(DocsTest.GENERATED_DOCUMENTATION_DIR, String.format("%s.csv", record.getKey()))), StandardCharsets.UTF_8)) {
                writer.write("¦type¦qualified name¦signature¦description\n");
                for (Row row : record.getValue()) {
                    String description = row.description.replaceAll("(\\{[a-zA-Z0-9_][a-zA-Z0-9_-]+})", "\\\\$1");
                    writer.write(String.format("¦%s¦%s¦%s¦%s\n", row.type, row.name, row.signature, description));
                }

            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        for (Map.Entry<String, List<Row>> record : namespaces.entrySet()) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(DocsTest.GENERATED_DOCUMENTATION_DIR, String.format("%s-lite.csv", record.getKey()))), StandardCharsets.UTF_8)) {
                writer.write("¦signature\n");
                for (Row row : record.getValue()) {
                    writer.write(String.format("¦%s\n", row.signature));
                }

            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

    }

    private void writeRowLight(Row row) {
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(DocsTest.GENERATED_DOCUMENTATION_DIR, String.format("%s-lite.csv", row.name))), StandardCharsets.UTF_8)) {
            writer.write("¦signature\n");
            writer.write(String.format("¦%s\n", row.signature));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void writeIndividualCsvs() {
        rows.forEach(this::writeRow);
        rows.forEach(this::writeRowLight);
    }

    public void writeNavAndOverview() {
        Map<String, List<Row>> topLevelNamespaces = topLevelNamespaces();

        try (Writer overviewWriter = new OutputStreamWriter(new FileOutputStream(new File(DocsTest.GENERATED_PARTIALS_DOCUMENTATION_DIR, "documentation.adoc")), StandardCharsets.UTF_8);
             Writer navWriter = new OutputStreamWriter(new FileOutputStream(new File(DocsTest.GENERATED_PARTIALS_DOCUMENTATION_DIR, "nav.adoc")), StandardCharsets.UTF_8)) {
            writeAutoGeneratedHeader(overviewWriter);
            writeAutoGeneratedHeader(navWriter);

            topLevelNamespaces.keySet().stream().sorted().forEach(topLevelNamespace -> {
                writeTopLevelNamespacePage(topLevelNamespaces, topLevelNamespace);

                try {
                    if (topLevelNamespaces.get(topLevelNamespace).size() < 3) {
                        overviewWriter.write("[discrete]\n");
                    }

                    overviewWriter.write("== xref::overview/" + topLevelNamespace + "/index.adoc[]\n\n");
                    overviewWriter.write(header());

                    navWriter.write("** xref::overview/" + topLevelNamespace + "/index.adoc[]\n");
                    for (Row row : topLevelNamespaces.get(topLevelNamespace)) {
                        String releaseType = extended.contains(row.name) ? "full" : "core";
                        String description = row.description
                                .replace("|", "\\|")
                                .replaceAll("(\\{[a-zA-Z0-9_][a-zA-Z0-9_-]+})", "\\\\$1");
                        overviewWriter.write(String.format("|%s\n|%s\n|%s\n",
                                String.format("%s[%s icon:book[]]\n\n%s", "xref::overview/" + topLevelNamespace + "/" + row.name + ".adoc", row.name, description),
                                String.format("label:%s[]", row.type),
                                String.format("label:apoc-%s[]", releaseType)));
                        navWriter.write("*** xref::overview/" + topLevelNamespace + "/" + row.name + ".adoc[]\n");
                    }

                    overviewWriter.write(footer());


                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
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

    private void writeAutoGeneratedHeader(Writer overviewWriter) throws IOException {
        overviewWriter.write("////\nThis file is generated by DocsTest, so don't change it!\n////\n\n");
    }

    private void writeTopLevelNamespacePage(Map<String, List<Row>> topLevelNamespaces, String topLevelNamespace) {
        new File(DocsTest.GENERATED_OVERVIEW_DIR, topLevelNamespace).mkdirs();

        try (Writer sectionWriter = new OutputStreamWriter(new FileOutputStream(new File(new File(DocsTest.GENERATED_OVERVIEW_DIR, topLevelNamespace), "index.adoc")), StandardCharsets.UTF_8)) {
            writeAutoGeneratedHeader(sectionWriter);
            sectionWriter.write("= " + topLevelNamespace + "\n");
            sectionWriter.write(":description: This section contains reference documentation for the " + topLevelNamespace + " procedures.\n\n");

            sectionWriter.write(header());

            for (Row row : topLevelNamespaces.get(topLevelNamespace)) {
                String releaseType = extended.contains(row.name) ? "full" : "core";
                String description = row.description
                        .replace("|", "\\|")
                        .replaceAll("(\\{[a-zA-Z0-9_][a-zA-Z0-9_-]+})", "\\\\$1");
                sectionWriter.write(String.format("|%s\n|%s\n|%s\n",
                        String.format("xref::%s[%s icon:book[]]\n\n%s", "overview/" + topLevelNamespace + "/" + row.name + ".adoc", row.name, description),
                        String.format("label:%s[]", row.type),
                        String.format("label:apoc-%s[]", releaseType)));
            }

            sectionWriter.write(footer());

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void writeProcedurePages() {
        for (ProcedureSignature procedure : allProcedures()) {
//            String[] parts = procedure.name().toString().split("\\.");
//            String topLevelDirectory = String.format("%s.%s", parts[0], parts[1]);

            String[] parts = procedure.name().toString().split("\\.");
            parts = Arrays.copyOf(parts, Math.min(2, parts.length-1));
            String topLevelDirectory = String.join(".", parts);

            new File(DocsTest.GENERATED_OVERVIEW_DIR, topLevelDirectory).mkdirs();

            writeProcedurePage(procedure, topLevelDirectory);
        }
    }

    public void writeFunctionPages() {
        allFunctions().forEach(userFunctionSignature -> {
//            String[] parts = userFunctionSignature.name().toString().split("\\.");
//            String topLevelDirectory = String.format("%s.%s", parts[0], parts[1]);

            String[] parts = userFunctionSignature.name().toString().split("\\.");
            parts = Arrays.copyOf(parts, Math.min(2, parts.length-1));
            String topLevelDirectory = String.join(".", parts);

            new File(DocsTest.GENERATED_OVERVIEW_DIR, topLevelDirectory).mkdirs();

            writeFunctionPage(userFunctionSignature, topLevelDirectory);
        });
    }

    private void writeFunctionPage(UserFunctionSignature userFunctionSignature, String topLevelDirectory) {
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(new File(DocsTest.GENERATED_OVERVIEW_DIR, topLevelDirectory), userFunctionSignature.name().toString() + ".adoc")), StandardCharsets.UTF_8)) {
            writeIndividualPageHeader(writer, userFunctionSignature.name().toString(), "function");
            writeLabel(writer, userFunctionSignature.name(), "function", userFunctionSignature.deprecated().isPresent());
            writeDescription(writer, userFunctionSignature.description());
            writeSignature(writer, userFunctionSignature.toString());
            writeInputParameters(writer, userFunctionSignature.inputSignature());
            writeConfigParameters(writer, userFunctionSignature.name().toString());
            writeEmailDependencies(writer, userFunctionSignature.name().toString());
            writeUsageExample(writer, userFunctionSignature.name().toString());
            writeExtraDocumentation(writer, userFunctionSignature.name());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void writeProcedurePage(ProcedureSignature procedure, String topLevelDirectory) {
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(new File(DocsTest.GENERATED_OVERVIEW_DIR, topLevelDirectory), procedure.name().toString() + ".adoc")), StandardCharsets.UTF_8)) {
            writeIndividualPageHeader(writer, procedure.name().toString(), "procedure");
            writeLabel(writer, procedure.name(), "procedure", procedure.deprecated().isPresent());
            writeDescription(writer, procedure.description());
            writeSignature(writer, procedure.toString());
            writeInputParameters(writer, procedure.inputSignature());
            writeConfigParameters(writer, procedure.name().toString());
            writeOutputParameters(procedure, writer);
            writeReadingFromFile(writer, procedure.name().toString());
            writeExportToFile(writer, procedure.name().toString());
            writeExportToStream(writer, procedure.name().toString());
            writeUuid(writer, procedure.name().toString());
            writeTTL(writer, procedure.name().toString());
            writeTrigger(writer, procedure.name().toString());
            writeNlpDependencies(writer, procedure.name().toString());
            writeMongodbDependencies(writer, procedure.name().toString());
            writeUsageExample(writer, procedure.name().toString());
            writeExtraDocumentation(writer, procedure.name());

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private final List<String> readsFromFile = Arrays.asList(
            "apoc.cypher.runFile", "apoc.cypher.runFiles", "apoc.cypher.runSchemaFile", "apoc.cypher.runSchemaFiles",
            "apoc.load.json", "apoc.load.jsonParams", "apoc.load.csv", "apoc.import.graphml", "apoc.import.xml"
    );
    private final List<String> writeToFile = Arrays.asList(
            "apoc.export.json.all", "apoc.export.json.data", "apoc.export.json.graph", "apoc.export.json.query",
            "apoc.export.cypher.all", "apoc.export.cypher.data", "apoc.export.cypher.query"
    );
    private final List<String> writeToStream = Arrays.asList(
            "apoc.export.json.all", "apoc.export.json.data", "apoc.export.json.graph", "apoc.export.json.query",
            "apoc.export.cypher.all", "apoc.export.cypher.data", "apoc.export.cypher.query"
    );

    private void writeReadingFromFile(Writer writer, String name) throws IOException {
        if(readsFromFile.contains(name)) {
            writer.write("== Reading from a file\n");
            writer.write("include::../../import/includes/enableFileImport.adoc[]\n\n");
        }
    }

    private void writeExportToFile(Writer writer, String name) throws IOException {
        if(writeToFile.contains(name)) {
            writer.write("== Exporting to a file\n");
            writer.write("include::partial$enableFileExport.adoc[]\n\n");
        }
    }

    private void writeExportToStream(Writer writer, String name) throws IOException {
        if(writeToStream.contains(name)) {
            writer.write("== Exporting a stream\n");
            writer.write("include::partial$streamExport.adoc[]\n\n");
        }
    }

    private void writeUuid(Writer writer, String name) throws IOException {
        if(name.startsWith("apoc.uuid")) {
            writer.write("== Enable automatic UUIDs\n");
            writer.write("include::partial$uuids.adoc[]\n\n");
        }
    }

    private void writeTTL(Writer writer, String name) throws IOException {
        if(name.startsWith("apoc.ttl")) {
            writer.write("== Enable TTL\n");
            writer.write("include::partial$ttl.adoc[]\n\n");
        }
    }

    private void writeTrigger(Writer writer, String name) throws IOException {
        if(name.startsWith("apoc.trigger")) {
            writer.write("== Enable Triggers\n");
            writer.write("include::partial$triggers.adoc[]\n\n");
        }
    }

    private void writeNlpDependencies(Writer writer, String name) throws IOException {
        if(name.startsWith("apoc.nlp")) {
            writer.write("== Install Dependencies\n");
            writer.write("include::partial$nlp-dependencies.adoc[]\n\n");
            writer.write("== Setting up API Key\n");

            String[] parts = name.split("\\.");
            parts = Arrays.copyOf(parts, 3);
            String platform = String.join(".", parts);

            writer.write("include::partial$nlp-api-keys-" + platform + ".adoc[]\n\n");
        }
    }

    private void writeMongodbDependencies(Writer writer, String name) throws IOException {
        if(name.startsWith("apoc.mongodb")) {
            writer.write("== Install Dependencies\n");
            writer.write("include::partial$mongodb-dependencies.adoc[]\n\n");
        }
    }

    private void writeEmailDependencies(Writer writer, String name) throws IOException {
        if(name.startsWith("apoc.data.email")) {
            writer.write("== Install Dependencies\n");
            writer.write("include::partial$email-dependencies.adoc[]\n\n");
        }
    }

    private void writeConfigParameters(Writer writer, String name) throws IOException {
        if(new File("../docs/asciidoc/modules/ROOT/partials/usage/config", name + ".adoc").exists()) {
            writer.write("== Config parameters\n");
            writer.write("include::partial$usage/config/" + name + ".adoc[]\n\n");
        }
    }

    private void writeUsageExample(Writer writer, String name) throws IOException {
        if(new File("../docs/asciidoc/modules/ROOT/partials/usage", name + ".adoc").exists()) {
            writer.write("[[usage-" + name + "]]\n== Usage Examples\n");
            writer.write("include::partial$usage/" + name + ".adoc[]\n\n");
        }
    }

    Map<String, List<Row>> topLevelNamespaces() {
        return rows.stream().collect(Collectors.groupingBy(value -> {
            String[] parts = value.name.split("\\.");
            parts = Arrays.copyOf(parts, Math.min(2, parts.length-1));
            return String.join(".", parts);
        }));
    }


    private void writeRow(Row row) {
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(new File(DocsTest.GENERATED_DOCUMENTATION_DIR, String.format("%s.csv", row.name))), StandardCharsets.UTF_8)) {
            writer.write("¦type¦qualified name¦signature¦description\n");
            String description = row.description.replaceAll("(\\{[a-zA-Z0-9_][a-zA-Z0-9_-]+})", "\\\\$1");
            writer.write(String.format("¦%s¦%s¦%s¦%s\n", row.type, row.name, row.signature, description));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void writeExtraDocumentation(Writer writer, QualifiedName name) throws IOException {
        Optional<String> documentation = findExtraDocumentations(name);
        if (documentation.isPresent()) {
            writer.write(documentation.get() + "[More documentation of " + name.toString() + ",role=more information]\n\n");
        }
    }

    @NotNull
    private String extractDescription(Optional<String> description) {
        return description
                .orElse("")
                .replaceAll("(\\{[a-zA-Z0-9_][a-zA-Z0-9_-]+})", "\\\\$1");
    }

    private void writeOutputParameters(ProcedureSignature procedure, Writer writer) throws IOException {
        if (procedure.outputSignature().size() > 0) {
            writeOutputParametersHeader(writer);

            for (FieldSignature fieldSignature : procedure.outputSignature()) {
                writer.write(String.format("|%s|%s\n", fieldSignature.name(), fieldSignature.neo4jType().toString()));
            }
            writeParametersFooter(writer);
        }
    }

    private void writeInputParameters(Writer writer, List<FieldSignature> fieldSignatures) throws IOException {
        List<FieldSignature> x = fieldSignatures;
        if (x.size() > 0) {
            writeInputParametersHeader(writer);
            for (FieldSignature fieldSignature : x) {
                writer.write(String.format("|%s|%s|%s\n", fieldSignature.name(), fieldSignature.neo4jType().toString(), fieldSignature.defaultValue().map(DefaultParameterValue::value).orElse(null)));
            }
            writeParametersFooter(writer);
        }
    }

    private void writeSignature(Writer writer, String name) throws IOException {
        writer.write("== Signature\n\n");
        writer.write("[source]\n----\n" + name + "\n----\n\n");
    }

    private void writeLabel(Writer writer, QualifiedName name, String type, boolean isDeprecated) throws IOException {
        String release = extended.contains(name.toString()) ? "full" : "core";
        writer.write("label:" + type + "[] label:apoc-" + release + "[]" + (isDeprecated ? " label:deprecated[]" : "") + "\n\n");
    }

    private void writeDescription(Writer writer, Optional<String> potentialDescription) throws IOException {
        String description = extractDescription(potentialDescription);
        if (!description.isBlank()) {
            writer.write("[.emphasis]\n" + description.trim() + "\n\n");
        }
    }

    private void writeIndividualPageHeader(Writer writer, String thing, String type) throws IOException {
        writeAutoGeneratedHeader(writer);
        writer.write("= " + thing + "\n");
        writer.write(":description: This section contains reference documentation for the " + thing + " " + type + ".\n\n");
    }

    private List<Row> collectProcedures() {
        return db.executeTransactionally("CALL dbms.procedures() YIELD signature, name, description WHERE name STARTS WITH 'apoc' RETURN 'procedure' AS type, name, description, signature ORDER BY signature", Collections.emptyMap(),
                result -> result.stream().map(record -> new Row(
                        record.get("type").toString(),
                        record.get("name").toString(),
                        record.get("signature").toString(),
                        record.get("description").toString())
                ).collect(Collectors.toList()));
    }

    private List<Row> collectFunctions() {
        return db.executeTransactionally("CALL dbms.functions() YIELD signature, name, description WHERE name STARTS WITH 'apoc' RETURN 'function' AS type, name, description, signature ORDER BY signature", Collections.emptyMap(),
                result -> result.stream().map(record -> new Row(
                        record.get("type").toString(),
                        record.get("name").toString(),
                        record.get("signature").toString(),
                        record.get("description").toString())
                ).collect(Collectors.toList()));
    }

    private void writeParametersFooter(Writer writer) throws IOException {
        writer.write("|===\n\n");
    }

    private void writeInputParametersHeader(Writer writer) throws IOException {
        writer.write("== Input parameters\n");
        writer.write("[.procedures, opts=header]\n" +
                "|===\n" +
                "| Name | Type | Default \n");
    }

    private void writeOutputParametersHeader(Writer writer) throws IOException {
        writer.write("== Output parameters\n");
        writer.write("[.procedures, opts=header]\n" +
                "|===\n" +
                "| Name | Type \n");
    }

    @NotNull
    private Optional<String> findExtraDocumentations(QualifiedName name) {
        return docs.keySet().stream()
                .filter((key) -> Pattern.compile(key).matcher(name.toString()).matches())
                .map(value -> String.format("xref::%s", docs.get(value)))
                .findFirst();
    }

    @NotNull
    private Set<ProcedureSignature> allProcedures() {
        GlobalProcedures globalProcedures = resolver.resolveDependency(GlobalProcedures.class);
        Set<ProcedureSignature> allProcedures = globalProcedures.getAllProcedures().stream().filter(signature -> signature.name().toString().startsWith("apoc")).collect(Collectors.toSet());
        return allProcedures;
    }

    @NotNull
    private Stream<UserFunctionSignature> allFunctions() {
        Stream<UserFunctionSignature> loadedFunctions = resolver.resolveDependency(GlobalProcedures.class).getAllNonAggregatingFunctions().filter(signature -> signature.name().toString().startsWith("apoc"));
        Stream<UserFunctionSignature> loadedAggregationFunctions = resolver.resolveDependency(GlobalProcedures.class).getAllAggregatingFunctions().filter(signature -> signature.name().toString().startsWith("apoc"));
        return Stream.concat(loadedFunctions, loadedAggregationFunctions);
    }
}
