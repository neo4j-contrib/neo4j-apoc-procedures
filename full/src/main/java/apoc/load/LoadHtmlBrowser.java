/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.load;

import static java.util.Optional.ofNullable;

import io.github.bonigarcia.wdm.WebDriverManager;
import io.github.bonigarcia.wdm.config.Architecture;
import io.github.bonigarcia.wdm.config.OperatingSystem;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;


public class LoadHtmlBrowser {

    public static InputStream getChromeInputStream(
            String url,
            Map<String, String> query,
            LoadHtmlConfig config,
            boolean isHeadless,
            boolean isAcceptInsecureCerts) {
        setupWebDriverManager(WebDriverManager.chromedriver(), config);

        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.addArguments("--headless=new");
        chromeOptions.setAcceptInsecureCerts(isAcceptInsecureCerts);
        return getInputStreamWithBrowser(url, query, config, new ChromeDriver(chromeOptions));
    }

    public static InputStream getFirefoxInputStream(
            String url,
            Map<String, String> query,
            LoadHtmlConfig config,
            boolean isHeadless,
            boolean isAcceptInsecureCerts) {
        setupWebDriverManager(WebDriverManager.firefoxdriver(), config);

        FirefoxOptions firefoxOptions = new FirefoxOptions();
        firefoxOptions.addArguments("-headless");
        firefoxOptions.setAcceptInsecureCerts(isAcceptInsecureCerts);
        return getInputStreamWithBrowser(url, query, config, new FirefoxDriver(firefoxOptions));
    }

    private static void setupWebDriverManager(WebDriverManager driver, LoadHtmlConfig config) {
        // strings
        ofNullable(config.getDriverVersion()).ifPresent(driver::driverVersion);
        ofNullable(config.getBrowserVersion()).ifPresent(driver::browserVersion);
        ofNullable(config.getCachePath()).ifPresent(driver::cachePath);
        ofNullable(config.getResolutionCachePath()).ifPresent(driver::resolutionCachePath);
        ofNullable(config.getProxy()).ifPresent(driver::proxy);
        ofNullable(config.getProxyUser()).ifPresent(driver::proxyUser);
        ofNullable(config.getProxyPass()).ifPresent(driver::proxyPass);
        ofNullable(config.getGitHubToken()).ifPresent(driver::gitHubToken);

        // URLs
        ofNullable(config.getDriverRepositoryUrl()).ifPresent(c -> driver.driverRepositoryUrl(fromUrl(c)));
        ofNullable(config.getVersionsPropertiesUrl()).ifPresent(c -> driver.versionsPropertiesUrl(fromUrl(c)));
        ofNullable(config.getCommandsPropertiesUrl()).ifPresent(c -> driver.commandsPropertiesUrl(fromUrl(c)));

        // enums
        ofNullable(config.getOperatingSystem()).ifPresent(c -> driver.operatingSystem(OperatingSystem.valueOf(c)));
        ofNullable(config.getArchitecture()).ifPresent(c -> driver.architecture(Architecture.valueOf(c)));

        // booleans
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

        // ints
        ofNullable(config.getTimeout()).ifPresent(driver::timeout);
        ofNullable(config.getTtl()).ifPresent(driver::ttl);
        ofNullable(config.getTtlBrowsers()).ifPresent(driver::ttlBrowsers);

        driver.setup();
    }

    private static URL fromUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static InputStream getInputStreamWithBrowser(
            String url, Map<String, String> query, LoadHtmlConfig config, WebDriver driver) {
        driver.get(url);

        final long wait = config.getWait();
        if (wait > 0) {
            Wait<WebDriver> driverWait = new WebDriverWait(driver, Duration.ofSeconds(wait));
            try {
                driverWait.until(webDriver -> query.values().stream()
                        .noneMatch(selector ->
                                webDriver.findElements(By.cssSelector(selector)).isEmpty()));
            } catch (org.openqa.selenium.TimeoutException ignored) {
                // We continue the execution even if 1 or more elements were not found
            }
        }
        InputStream stream = IOUtils.toInputStream(driver.getPageSource(), config.getCharset());
        driver.close();
        return stream;
    }
}
