package apoc.result;

public class AssertSchemaResult {
    public final String label;
    public final String key;
    public boolean unique = false;
    public String action = "KEPT";

    public AssertSchemaResult(String label, String key) {
        this.label = label;
        this.key = key;
    }

    public AssertSchemaResult unique() {
        this.unique = true;
        return this;
    }

    public AssertSchemaResult dropped() {
        this.action = "DROPPED";
        return this;
    }

    public AssertSchemaResult created() {
        this.action = "CREATED";
        return this;
    }
}
