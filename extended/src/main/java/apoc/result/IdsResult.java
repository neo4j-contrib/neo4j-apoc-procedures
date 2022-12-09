package apoc.result;

public class IdsResult {

    public long nodeIds;

    public long relIds;

    public long propIds;

    public long relTypeIds;

    public IdsResult(long nodeIds, long relIds, long propIds, long relTypeIds) {
        this.nodeIds = nodeIds;
        this.relIds = relIds;
        this.propIds = propIds;
        this.relTypeIds = relTypeIds;
    }

}
