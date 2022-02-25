package apoc.load;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class LoadHtmlConfig {
    enum Browser { NONE, CHROME, FIREFOX }
    enum FailSilently { FALSE, WITH_LOG, WITH_LIST }

    private final boolean headless;
    private final boolean acceptInsecureCerts;
    private final boolean children;
    private final boolean htmlString;
    
    private final String charset;
    private final String baseUri;
    
    private final Browser browser;
    private final FailSilently failSilently;
    
    private final long wait;
    private final int textSize;
    
    public LoadHtmlConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.headless = Util.toBoolean(config.getOrDefault("headless", true));
        this.acceptInsecureCerts = Util.toBoolean(config.getOrDefault("acceptInsecureCerts", true));
        this.children = Util.toBoolean(config.getOrDefault("children", false));
        this.charset = (String) config.getOrDefault("charset", "UTF-8"); 
        this.baseUri = (String) config.getOrDefault("baseUri", ""); 
        this.browser = Browser.valueOf((String) config.getOrDefault("browser", Browser.NONE.toString()));
        this.failSilently = FailSilently.valueOf((String) config.getOrDefault("failSilently", FailSilently.FALSE.toString()));
        this.wait = Util.toLong(config.getOrDefault("wait", 0));
        this.textSize = Util.toInteger(config.getOrDefault("textSize", 80));
        this.htmlString = Util.toBoolean(config.get("htmlString"));
    }

    public boolean isHeadless() {
        return headless;
    }

    public boolean isAcceptInsecureCerts() {
        return acceptInsecureCerts;
    }

    public boolean isChildren() {
        return children;
    }

    public String getCharset() {
        return charset;
    }

    public String getBaseUri() {
        return baseUri;
    }

    public Browser getBrowser() {
        return browser;
    }

    public FailSilently getFailSilently() {
        return failSilently;
    }

    public long getWait() {
        return wait;
    }

    public int getTextSize() {
        return textSize;
    }

    public boolean isHtmlString() {
        return htmlString;
    }
}
