package apoc.script;

import javax.script.ScriptEngineFactory;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Victor Borja vborja@apache.org
 * @since 12.06.16
 */
public class EnginesResult {

    public final String name;
    public final String version;
    public final String languageName;
    public final String languageVersion;
    public final String names;

    public EnginesResult(ScriptEngineFactory factory) {
        this.name = factory.getEngineName();
        this.version = factory.getEngineVersion();
        this.languageName = factory.getLanguageName();
        this.languageVersion = factory.getLanguageVersion();
        this.names = factory.getNames().stream().collect(Collectors.joining(","));
    }

    public EnginesResult(Map<String, Object> row) {
        this.name = (String) row.get("name");
        this.version = (String) row.get("version");
        this.languageName = (String) row.get("languageName");
        this.languageVersion = (String) row.get("languageVersion");
        this.names = (String) row.get("names");
    }

    @Override
    public String toString() {
        return "EnginesResult{" +
                "name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", languageName='" + languageName + '\'' +
                ", languageVersion='" + languageVersion + '\'' +
                ", names='" + names + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EnginesResult that = (EnginesResult) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        if (languageName != null ? !languageName.equals(that.languageName) : that.languageName != null) return false;
        if (languageVersion != null ? !languageVersion.equals(that.languageVersion) : that.languageVersion != null)
            return false;
        return names != null ? names.equals(that.names) : that.names == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (languageName != null ? languageName.hashCode() : 0);
        result = 31 * result + (languageVersion != null ? languageVersion.hashCode() : 0);
        result = 31 * result + (names != null ? names.hashCode() : 0);
        return result;
    }
}
