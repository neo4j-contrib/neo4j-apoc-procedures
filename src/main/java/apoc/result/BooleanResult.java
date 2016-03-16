package apoc.result;

/**
 * @author mh
 * @since 15.03.16
 */
public class BooleanResult {
    public static BooleanResult TRUE = new BooleanResult(true);
    public static BooleanResult FALSE = new BooleanResult(false);
    public final boolean value;

    public BooleanResult(boolean value) {
        this.value = value;
    }
}
