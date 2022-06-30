package apoc.load;

import apoc.util.Util;

import java.util.Collections;
import java.util.Map;

public class LoadHtmlConfig {
    private final String driverVersion;
    private final String browserVersion;
    private final String architecture;
    private final String operatingSystem;
    private final String driverRepositoryUrl;
    private final String versionsPropertiesUrl;
    private final String commandsPropertiesUrl;
    private final String cachePath;
    private final String resolutionCachePath;
    private final String proxy;
    private final String proxyUser;
    private final String proxyPass;
    private final String gitHubToken;
    private final Integer ttlBrowsers;
    private final Integer timeout;
    private final boolean useLocalVersionsPropertiesFirst;
    private final boolean clearDriverCache;
    private final boolean forceDownload;
    private final boolean avoidExport;
    private final boolean avoidReadReleaseFromRepository;
    private final boolean useBetaVersions;
    private final boolean useMirror;
    private final boolean clearResolutionCache;
    private final Integer ttl;
    private final boolean avoidOutputTree;
    private final boolean avoidFallback;
    private final boolean avoidBrowserDetection;
    private final boolean avoidTmpFolder;

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
        this.children = Util.toBoolean(config.get("children"));
        this.charset = (String) config.getOrDefault("charset", "UTF-8"); 
        this.baseUri = (String) config.getOrDefault("baseUri", ""); 
        this.browser = Browser.valueOf((String) config.getOrDefault("browser", Browser.NONE.toString()));
        this.failSilently = FailSilently.valueOf((String) config.getOrDefault("failSilently", FailSilently.FALSE.toString()));
        this.wait = Util.toLong(config.getOrDefault("wait", 0));
        this.textSize = Util.toInteger(config.getOrDefault("textSize", 80));
        this.htmlString = Util.toBoolean(config.get("htmlString"));

        this.driverVersion = (String) config.get("driverVersion");
        this.browserVersion = (String) config.get("browserVersion");
        this.architecture = (String) config.get("architecture");
        this.operatingSystem = (String) config.get("operatingSystem");
        this.driverRepositoryUrl = (String) config.get("driverRepositoryUrl");
        this.versionsPropertiesUrl = (String) config.get("versionsPropertiesUrl");
        this.commandsPropertiesUrl = (String) config.get("commandsPropertiesUrl");
        this.cachePath = (String) config.get("cachePath");
        this.resolutionCachePath = (String) config.get("resolutionCachePath");
        this.proxy = (String) config.get("proxy");
        this.proxyUser = (String) config.get("proxyUser");
        this.proxyPass = (String) config.get("proxyPass");
        this.gitHubToken = (String) config.get("gitHubToken");

        this.forceDownload = Util.toBoolean(config.get("forceDownload"));
        this.useBetaVersions = Util.toBoolean(config.get("useBetaVersions"));
        this.useMirror = Util.toBoolean(config.get("useMirror"));
        this.avoidExport = Util.toBoolean(config.get("avoidExport"));
        this.avoidOutputTree = Util.toBoolean(config.get("avoidOutputTree"));
        this.clearDriverCache = Util.toBoolean(config.get("clearDriverCache"));
        this.clearResolutionCache = Util.toBoolean(config.get("clearResolutionCache"));
        this.avoidFallback = Util.toBoolean(config.get("avoidFallback"));
        this.avoidBrowserDetection = Util.toBoolean(config.get("avoidBrowserDetection"));
        this.avoidReadReleaseFromRepository = Util.toBoolean(config.get("avoidReadReleaseFromRepository"));
        this.avoidTmpFolder = Util.toBoolean(config.get("avoidTmpFolder"));
        this.useLocalVersionsPropertiesFirst = Util.toBoolean(config.get("useLocalVersionsPropertiesFirst"));

        this.timeout = Util.toInteger(config.get("timeout"));
        this.ttl = Util.toInteger(config.get("ttl"));
        this.ttlBrowsers = Util.toInteger(config.get("ttlBrowsers"));
        
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

    public boolean isHtmlString() {
        return htmlString;
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
    
    public int getTextSize() {
        return textSize;
    }

    public long getWait() {
        return wait;
    }

    public String getDriverVersion() {
        return driverVersion;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public String getArchitecture() {
        return architecture;
    }

    public String getOperatingSystem() {
        return operatingSystem;
    }

    public String getDriverRepositoryUrl() {
        return driverRepositoryUrl;
    }

    public String getVersionsPropertiesUrl() {
        return versionsPropertiesUrl;
    }

    public String getCommandsPropertiesUrl() {
        return commandsPropertiesUrl;
    }

    public String getCachePath() {
        return cachePath;
    }

    public String getResolutionCachePath() {
        return resolutionCachePath;
    }

    public String getProxy() {
        return proxy;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public String getProxyPass() {
        return proxyPass;
    }

    public Integer getTtlBrowsers() {
        return ttlBrowsers;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public String getGitHubToken() {
        return gitHubToken;
    }

    public boolean isUseLocalVersionsPropertiesFirst() {
        return useLocalVersionsPropertiesFirst;
    }

    public boolean isClearDriverCache() {
        return clearDriverCache;
    }

    public boolean isForceDownload() {
        return forceDownload;
    }

    public boolean isAvoidExport() {
        return avoidExport;
    }

    public boolean isAvoidReadReleaseFromRepository() {
        return avoidReadReleaseFromRepository;
    }

    public boolean isUseBetaVersions() {
        return useBetaVersions;
    }

    public boolean isUseMirror() {
        return useMirror;
    }

    public boolean isClearResolutionCache() {
        return clearResolutionCache;
    }

    public Integer getTtl() {
        return ttl;
    }

    public boolean isAvoidOutputTree() {
        return avoidOutputTree;
    }

    public boolean isAvoidFallback() {
        return avoidFallback;
    }

    public boolean isAvoidBrowserDetection() {
        return avoidBrowserDetection;
    }

    public boolean isAvoidTmpFolder() {
        return avoidTmpFolder;
    }
}
