package apoc.load;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(Parameterized.class)
public class SimpleXmlTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"some text with spaces", Arrays.asList("some" ," ", "text", " ", "with", " ", "spaces")},
                {" some text with spaces", Arrays.asList(" ", "some" ," ", "text", " ", "with", " ", "spaces")}
        });
    }

    private String input;
    private List<String> expected;

    public SimpleXmlTest(String input, List<String> expected) {
        this.input = input;
        this.expected = expected;
    }

    @Test
    public void testParseTextIntoPartsAndDelimiters() {
        Xml cut = new Xml();
        List<String> result = cut.parseTextIntoPartsAndDelimiters(input, Pattern.compile("\\s"));

        MatcherAssert.assertThat(result, Matchers.equalTo(expected));
    }
}
