package apoc.result;

public class ErrorResult {
    public final String code;
    public final String message;

    public ErrorResult(String code, String message) {
        this.code = code;
        this.message = message;
    }
}
