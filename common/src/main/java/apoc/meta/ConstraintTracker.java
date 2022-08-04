package apoc.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConstraintTracker {
    // The following maps are (label|rel-type)/constraintdefinition entries

    public static final Map<String, List<String>> relConstraints = new HashMap<>(20);;
    public static final Map<String, List<String>> nodeConstraints = new HashMap<>(20);;
}