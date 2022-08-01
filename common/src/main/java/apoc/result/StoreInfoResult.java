package apoc.result;

public class StoreInfoResult {

    public long logSize;

    public long stringStoreSize;

    public long arrayStoreSize;

    public long relStoreSize;

    public long propStoreSize;

    public long totalStoreSize;

    public long nodeStoreSize;

    public StoreInfoResult(
            long logSize,
            long stringStoreSize,
            long arrayStoreSize,
            long relStoreSize,
            long propStoreSize,
            long totalStoreSize,
            long nodeStoreSize
    ) {
        this.logSize = logSize;
        this.stringStoreSize = stringStoreSize;
        this.arrayStoreSize = arrayStoreSize;
        this.relStoreSize = relStoreSize;
        this.propStoreSize = propStoreSize;
        this.totalStoreSize = totalStoreSize;
        this.nodeStoreSize = nodeStoreSize;
    }

}
