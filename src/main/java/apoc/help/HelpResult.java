package apoc.help;

/**
 * @author mh
 * @since 11.04.16
 */
public class HelpResult {
    public String name;
    public String text;
    public boolean writes;

    public HelpResult(String name, String text, boolean writes) {
        this.name = name;
        this.text = text;
        this.writes = writes;
    }
}
