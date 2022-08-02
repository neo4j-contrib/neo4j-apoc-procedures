package apoc.result;

public class CompareIdxToConsRels extends CompareIdxToCons {

    public String type;

    public CompareIdxToConsRels(String type) {
        super();
        this.type = type;
    }
    
    @Override
    public String getLabelOrType() {
        return type;
    }
}
