package apoc.dv;

import java.util.List;

public class VRResult {
    public String name;
    public String type;
    public String URL;
    public String desc;
    public List<String> params;
    public List<String> labels;
    public String query;

    public VRResult(String name, String type, String URL, String query, String desc, List<String> params, List<String> labels) {
        this.name = name;
        this.type = type;
        this.URL = URL;
        this.desc = desc;
        this.params = params;
        this.labels = labels;
        this.query = query;
    }
}
