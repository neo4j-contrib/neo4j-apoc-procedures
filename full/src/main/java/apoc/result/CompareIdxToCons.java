package apoc.result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class CompareIdxToCons {
    public List<String> commonProps;
    public Map<String, Object> onlyIdxProps;
    public Map<String, Object> onlyConstraintsProps;

    public CompareIdxToCons() {
        this.commonProps = new ArrayList<>();
        this.onlyIdxProps = new HashMap<>();
        this.onlyConstraintsProps = new HashMap<>();
    }

    public abstract String getLabelOrType();

    public void addCommonProps(List<String> commonProps) {
        this.commonProps.addAll(commonProps);
        // sort and remove duplicates 
        this.commonProps = this.commonProps.stream()
                .sorted()
                .distinct()
                .collect(Collectors.toList());
    }

    public void putOnlyIdxProps(List<String> properties, String name) {
        this.onlyIdxProps.put(name, properties);
    }

    public void putOnlyConstraintsProps(List<String> properties, String name) {
        this.onlyConstraintsProps.put(name, properties);
    }
}
