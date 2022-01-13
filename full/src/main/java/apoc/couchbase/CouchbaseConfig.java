package apoc.couchbase;

import apoc.util.Util;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.RawStringTranscoder;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.couchbase.CouchbaseManager.COUCHBASE_CONFIG_KEY;
import static com.couchbase.client.core.env.IoConfig.DEFAULT_DNS_SRV_ENABLED;
import static com.couchbase.client.core.env.IoConfig.DEFAULT_TCP_KEEPALIVE_ENABLED;
import static java.time.Duration.ofMillis;

public class CouchbaseConfig {

    private static final Map<String, Object> DEFAULT_CONFIG = Map.of("connectTimeout", 5000L,
            "kvTimeout", 2500,
            "ioPoolSize", 3);

    private final String collection;
    private final String scope;

    private final Long connectTimeout;
    private final Long kvTimeout;
    private final long queryTimeout;
    private final long analyticsTimeout;
    private final long disconnectTimeout;
    private final long viewTimeout;
    private final long searchTimeout;
    
    private final Integer compressionMinSize;
    private final Double compressionMinRatio;
    private final boolean compressionEnabled;
    
    private final boolean mutationTokensEnabled;
    
    private final RetryStrategy retryStrategy;
    
    private final Transcoder transcoder;
    private final long configPollInterval;
    private final long idleHttpConnectionTimeout;
    private final long tcpKeepAliveTime;
    private final boolean enableDnsSrv;
    private final boolean enableTcpKeepAlives;
    private final NetworkResolution networkResolution;
    private final String trustCertificate;
    private final Long waitUntilReady;

    public CouchbaseConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.collection = (String) config.getOrDefault("collection", CollectionIdentifier.DEFAULT_COLLECTION);
        this.scope = (String) config.getOrDefault("scope", CollectionIdentifier.DEFAULT_SCOPE);

        this.compressionMinSize = Util.toInteger(config.getOrDefault("compressionMinSize", CompressionConfig.DEFAULT_MIN_SIZE));
        this.compressionMinRatio = Util.toDouble(config.getOrDefault("compressionMinRatio", CompressionConfig.DEFAULT_MIN_RATIO));
        this.compressionEnabled = Util.toBoolean(config.getOrDefault("compressionEnabled", CompressionConfig.DEFAULT_ENABLED));
        
        this.mutationTokensEnabled = Util.toBoolean(config.getOrDefault("mutationTokensEnabled", true));
        
        this.retryStrategy = RetryConfig.valueOf(config.getOrDefault("retryStrategy", RetryConfig.BESTEFFORT.name()).toString().toUpperCase()).getInstance();
        
        this.transcoder = TrancoderConfig.valueOf(config.getOrDefault("transcoder", TrancoderConfig.DEFAULT.name()).toString().toUpperCase()).getInstance();

        this.connectTimeout = Util.toLong(config.get("connectTimeout"));
        this.kvTimeout = Util.toLong(config.get("kvTimeout"));

        this.disconnectTimeout = Util.toLong(config.getOrDefault("disconnectTimeout", 10000L));
        this.queryTimeout = Util.toLong(config.getOrDefault("queryTimeout", 75000L));
        this.analyticsTimeout = Util.toLong(config.getOrDefault("analyticsTimeout", 75000L));
        this.viewTimeout = Util.toLong(config.getOrDefault("viewTimeout", 75000L));
        this.searchTimeout = Util.toLong(config.getOrDefault("searchTimeout", 75000L));
        
        this.configPollInterval= Util.toLong(config.getOrDefault("configPollInterval", 2500L));
        this.idleHttpConnectionTimeout= Util.toLong(config.getOrDefault("idleHttpConnectionTimeout", 4500L));
        this.tcpKeepAliveTime= Util.toLong(config.getOrDefault("tcpKeepAliveTime", 60000L));
        this.enableDnsSrv= Util.toBoolean(config.getOrDefault("enableDnsSrv", DEFAULT_DNS_SRV_ENABLED));
        this.enableTcpKeepAlives= Util.toBoolean(config.getOrDefault("enableTcpKeepAlives", DEFAULT_TCP_KEEPALIVE_ENABLED));
        this.networkResolution= NetworkResolution.valueOf((String) config.get("networkResolution"));
        
        this.trustCertificate = (String) config.get("trustCertificate");
        this.waitUntilReady = Util.toLong(config.get("waitUntilReady"));
    }

    public Integer getCompressionMinSize() {
        return compressionMinSize;
    }

    public Double getCompressionMinRatio() {
        return compressionMinRatio;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    public Transcoder getTranscoder() {
        return transcoder;
    }

    public String getCollection() {
        return collection;
    }

    public String getScope() {
        return scope;
    }

    public boolean isMutationTokensEnabled() {
        return mutationTokensEnabled;
    }

    public Long getConnectTimeout() {
        return connectTimeout;
    }

    public Long getKvTimeout() {
        return kvTimeout;
    }

    public long getQueryTimeout() {
        return queryTimeout;
    }

    public long getAnalyticsTimeout() {
        return analyticsTimeout;
    }

    public long getDisconnectTimeout() {
        return disconnectTimeout;
    }

    public long getViewTimeout() {
        return viewTimeout;
    }

    public long getSearchTimeout() {
        return searchTimeout;
    }

    public long getConfigPollInterval() {
        return configPollInterval;
    }

    public long getIdleHttpConnectionTimeout() {
        return idleHttpConnectionTimeout;
    }

    public long getTcpKeepAliveTime() {
        return tcpKeepAliveTime;
    }

    public boolean isEnableDnsSrv() {
        return enableDnsSrv;
    }

    public NetworkResolution getNetworkResolution() {
        return networkResolution;
    }

    public String getTrustCertificate() {
        return trustCertificate;
    }

    public Long getWaitUntilReady() {
        return waitUntilReady;
    }

    public boolean isEnableTcpKeepAlives() {
        return enableTcpKeepAlives;
    }

    public ClusterEnvironment getEnv() {
        ClusterEnvironment.Builder builder = ClusterEnvironment.builder()
                .retryStrategy(retryStrategy);

        if (trustCertificate != null) {
            builder.securityConfig(SecurityConfig.builder()
                    .enableTls(true)
                    .trustCertificate(Path.of(trustCertificate)));
        }

        if (compressionEnabled) {
            final CompressionConfig.Builder compressionConfig = CompressionConfig.enable(true);
            compressionConfig.minSize(compressionMinSize);
            compressionConfig.minRatio(compressionMinRatio);
            builder.compressionConfig(compressionConfig);
        }

        // if null we take the default transcoder
        if (transcoder != null) {
            builder.transcoder(transcoder);
        }

        builder.ioConfig(IoConfig.enableMutationTokens(mutationTokensEnabled)
                .configPollInterval(ofMillis(configPollInterval))
                .idleHttpConnectionTimeout(ofMillis(idleHttpConnectionTimeout))
                .enableTcpKeepAlives(enableTcpKeepAlives)
                .tcpKeepAliveTime(ofMillis(tcpKeepAliveTime))
                .enableDnsSrv(enableDnsSrv)
                .networkResolution(networkResolution));

        final long connectTimeoutFromConf = connectTimeout != null
                ? connectTimeout
                : Long.parseLong(getConfig("connectTimeout"));

        final long kvTimeoutFromConf= kvTimeout != null
                ? kvTimeout
                : Long.parseLong(getConfig("kvTimeout"));

        builder.timeoutConfig(TimeoutConfig
                .connectTimeout(ofMillis(connectTimeoutFromConf))
                .kvTimeout(ofMillis(kvTimeoutFromConf))
                .queryTimeout(ofMillis(queryTimeout))
                .analyticsTimeout(ofMillis(analyticsTimeout))
                .disconnectTimeout(ofMillis(disconnectTimeout))
                .viewTimeout(ofMillis(viewTimeout))
                .searchTimeout(ofMillis(searchTimeout))
        );
        builder.ioEnvironment(IoEnvironment.builder().eventLoopThreadCount(
                Integer.parseInt(getConfig("ioPoolSize"))));

        return builder.build();
    }

    private String getConfig(String key) {
        return apocConfig().getString("apoc." + COUCHBASE_CONFIG_KEY + key, DEFAULT_CONFIG.get(key).toString());
    }

    enum RetryConfig {
        FAILFAST(FailFastRetryStrategy.INSTANCE),
        BESTEFFORT(BestEffortRetryStrategy.INSTANCE);

        private final RetryStrategy instance;

        RetryConfig(RetryStrategy instance) {
            this.instance = instance;
        }

        RetryStrategy getInstance() {
            return instance;
        }
    }

    enum TrancoderConfig {
        DEFAULT(null),
        RAWJSON(RawJsonTranscoder.INSTANCE),
        RAWSTRING(RawStringTranscoder.INSTANCE),
        RAWBINARY(RawBinaryTranscoder.INSTANCE);

        private final Transcoder instance;

        TrancoderConfig(Transcoder instance) {
            this.instance = instance;
        }

        Transcoder getInstance() {
            return instance;
        }
    }
}
