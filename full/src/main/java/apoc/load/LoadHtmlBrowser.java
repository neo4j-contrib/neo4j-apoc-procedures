package apoc.load;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.IOUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class LoadHtmlBrowser {
    
    public static InputStream getChromeInputStream(String url, Map<String, String> query, LoadHtmlConfig config, boolean isHeadless, boolean isAcceptInsecureCerts) {
        WebDriverManager.chromedriver().setup();
        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.setHeadless(isHeadless);
        chromeOptions.setAcceptInsecureCerts(isAcceptInsecureCerts);
        return getInputStreamWithBrowser(url, query, config, new ChromeDriver(chromeOptions));
    }
    
    public static InputStream getFirefoxInputStream(String url, Map<String, String> query, LoadHtmlConfig config, boolean isHeadless, boolean isAcceptInsecureCerts) {
        WebDriverManager.firefoxdriver()
                .driverVersion("0.30.0")
                .setup();
        FirefoxOptions firefoxOptions = new FirefoxOptions();
        firefoxOptions.setHeadless(isHeadless);
        firefoxOptions.setAcceptInsecureCerts(isAcceptInsecureCerts);
        return getInputStreamWithBrowser(url, query, config, new FirefoxDriver(firefoxOptions));
    }
    
    private static InputStream getInputStreamWithBrowser(String url, Map<String, String> query, LoadHtmlConfig config, WebDriver driver) {
        driver.get(url);

        final long wait = config.getWait();
        if (wait > 0) {
            Wait<WebDriver> driverWait = new WebDriverWait(driver, wait);
            try {
                driverWait.until(webDriver -> query.values().stream()
                        .noneMatch(selector -> webDriver.findElements(By.cssSelector(selector)).isEmpty()));
            } catch (org.openqa.selenium.TimeoutException ignored) {
                // We continue the execution even if 1 or more elements were not found
            }
        }
        InputStream stream = IOUtils.toInputStream(driver.getPageSource(), config.getCharset());
        driver.close();
        return stream;
    }
}
