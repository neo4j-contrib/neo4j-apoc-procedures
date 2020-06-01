package apoc.refactor;

/**
 * @author mh
 * @since 25.03.16
 */
public class RefactorResult {
    public long source;
    public long target;
    public String error;

    public RefactorResult(Long nodeId) {
        this.source = nodeId;
    }

    public RefactorResult withError(Exception e) {
        this.error = e.getMessage();
        return this;
    }

    public RefactorResult withOther(long nodeId) {
        this.target = nodeId;
        return this;
    }
}
