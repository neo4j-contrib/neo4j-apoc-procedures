package apoc.periodic;

public class LoopingBatchAndTotalResult {
    public Object loop;
    public long batches;
    public long total;

    public LoopingBatchAndTotalResult(Object loop, long batches, long total) {
        this.loop = loop;
        this.batches = batches;
        this.total = total;
    }
}
