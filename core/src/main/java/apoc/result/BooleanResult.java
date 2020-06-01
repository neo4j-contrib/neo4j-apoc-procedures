package apoc.result;

/**
 * @author mh
 * @since 15.03.16
 */
public class BooleanResult {
    public static final BooleanResult TRUE = new BooleanResult(true);
    public static final BooleanResult FALSE = new BooleanResult(false);
    public final Boolean value;

    public BooleanResult(Boolean value) {
        this.value = value;
    }
}
