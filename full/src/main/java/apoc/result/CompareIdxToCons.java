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

    public void addCommonProps(List<String> commonProps) {
        this.commonProps.addAll(commonProps);
        // sort and remove duplicates
        this.commonProps = this.commonProps.stream()
                .sorted()
                .distinct()
                .collect(Collectors.toList());
    }

    public void putOnlyIdxProps(String name, List<String> properties) {
        this.onlyIdxProps.put(name, properties);
    }

    public void putOnlyConstraintsProps(String name, List<String> properties) {
        this.onlyConstraintsProps.put(name, properties);
    }
}
