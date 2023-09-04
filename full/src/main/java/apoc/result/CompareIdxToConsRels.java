package apoc.result;

public class CompareIdxToConsRels extends CompareIdxToCons {

    public String relationshipType;

    public CompareIdxToConsRels(String relationshipType) {
        super();
        this.relationshipType = relationshipType;
    }
}
