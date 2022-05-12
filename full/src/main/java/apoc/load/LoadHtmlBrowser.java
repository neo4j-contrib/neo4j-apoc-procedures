package apoc.load;

import io.github.bonigarcia.wdm.WebDriverManager;
import io.github.bonigarcia.wdm.config.Architecture;
import io.github.bonigarcia.wdm.config.OperatingSystem;
import org.apache.commons.io.IOUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class LoadHtmlBrowser {
    
    public static InputStream getChromeInputStream(String url, Map<String, String> query, LoadHtmlConfig config, boolean isHeadless, boolean isAcceptInsecureCerts) {
        final WebDriverManager chromedriver = WebDriverManager.chromedriver();
//        chromedriver.setup();
        setupWebDriverManager(chromedriver, config);
        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.setHeadless(isHeadless);
        chromeOptions.setAcceptInsecureCerts(isAcceptInsecureCerts);
        return getInputStreamWithBrowser(url, query, config, new ChromeDriver(chromeOptions));
    }
    
    public static InputStream getFirefoxInputStream(String url, Map<String, String> query, LoadHtmlConfig config, boolean isHeadless, boolean isAcceptInsecureCerts) {
        final WebDriverManager firefoxdriver = WebDriverManager.firefoxdriver();
        setupWebDriverManager(firefoxdriver, config);
        
        FirefoxOptions firefoxOptions = new FirefoxOptions();
        firefoxOptions.setHeadless(isHeadless);
        firefoxOptions.setAcceptInsecureCerts(isAcceptInsecureCerts);
        return getInputStreamWithBrowser(url, query, config, new FirefoxDriver(firefoxOptions));
    }
    
    private static void setupWebDriverManager(WebDriverManager driver, LoadHtmlConfig config) {
        
        ofNullable(config.getDriverVersion()).ifPresentOrElse(driver::driverVersion, () -> {
            // currently we have to force default driver firefox version, because there is a bug with latest default driver
            if (config.getBrowser().equals(LoadHtmlConfig.Browser.FIREFOX)) {
                driver.driverVersion("0.3.0");
            }
        });
        
        ofNullable(config.getBrowserVersion())
                .ifPresent(driver::browserVersion);
        
        ofNullable(config.getArchitecture())
                .ifPresent(c -> driver.architecture(Architecture.valueOf(c)));
        ofNullable(config.getOperatingSystem())
                .ifPresent(c -> driver.operatingSystem(OperatingSystem.valueOf(c)));
        ofNullable(config.getDriverRepositoryUrl())
                .ifPresent(c -> driver.driverRepositoryUrl(fromUrl(c)));
        ofNullable(config.getVersionsPropertiesUrl())
                .ifPresent(c -> driver.versionsPropertiesUrl(fromUrl(c)));
        ofNullable(config.getCommandsPropertiesUrl())
                .ifPresent(c -> driver.commandsPropertiesUrl(fromUrl(c)));
        
        ofNullable(config.getCachePath())
                .ifPresent(driver::cachePath);
        ofNullable(config.getResolutionCachePath())
                .ifPresent(driver::resolutionCachePath);
        ofNullable(config.getProxy())
                .ifPresent(driver::proxy);
        ofNullable(config.getProxyUser())
                .ifPresent(driver::proxyUser);
        ofNullable(config.getProxyPass())
                .ifPresent(driver::proxyPass);
        
        ofNullable(config.getIgnoreDriverVersions())
                .ifPresent(cfg -> driver.ignoreDriverVersions(cfg.toArray(String[]::new)));
        
        if (config.isForceDownload()) {
            driver.forceDownload();
        }
        if (config.isUseBetaVersions()) {
            driver.useBetaVersions();
        }
        if (config.isUseMirror()) {
            driver.useMirror();
        }
        if (config.isAvoidExport()) {
            driver.avoidExport();
        }
        if (config.isAvoidOutputTree()) {
            driver.avoidOutputTree();
        }
        if (config.isClearDriverCache()) {
            driver.clearDriverCache();
        }
        if (config.isClearResolutionCache()) {
            driver.clearResolutionCache();
        }
        if (config.isAvoidFallback()) {
            driver.avoidFallback();
        }
        if (config.isAvoidBrowserDetection()) {
            driver.avoidBrowserDetection();
        }
        if (config.isAvoidReadReleaseFromRepository()) {
            driver.avoidReadReleaseFromRepository();
        }
        if (config.isAvoidTmpFolder()) {
            driver.avoidTmpFolder();
        }
        if (config.isUseLocalVersionsPropertiesFirst()) {
            driver.useLocalVersionsPropertiesFirst();
        }

        ofNullable(config.getTimeout())
                .ifPresent(driver::timeout);
        ofNullable(config.getTtl())
                .ifPresent(driver::ttl);
        ofNullable(config.getTtlBrowsers())
                .ifPresent(driver::ttlBrowsers);
        
        driver.setup();
    }
    
    private static URL fromUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
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
