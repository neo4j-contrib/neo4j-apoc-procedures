package apoc.result;

public class CompareIdxToConsNodes extends CompareIdxToCons {
    public String label;

    public CompareIdxToConsNodes(String label) {
        super();
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String getLabelOrType() {
        return label;
    }
}
