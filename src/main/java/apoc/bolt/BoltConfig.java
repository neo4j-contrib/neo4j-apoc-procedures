package apoc.bolt;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Config;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.internal.logging.JULogging;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class BoltConfig {
    private final boolean virtual;
    private final boolean addStatistics;
    private final boolean readOnly;
    private final boolean streamStatements;
    private final Config driverConfig;
    private final Map<String, Object> localParams;
    private final Map<String, Object> remoteParams;
    private final String databaseName;

    public BoltConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.virtual = (boolean) config.getOrDefault("virtual", false);
        this.addStatistics = (boolean) config.getOrDefault("statistics", false);
        this.readOnly = (boolean) config.getOrDefault("readOnly", true);
        this.streamStatements = (boolean) config.getOrDefault("streamStatements", false);
        this.databaseName = (String) config.getOrDefault("databaseName", "neo4j");
        this.driverConfig = toDriverConfig((Map<String, Object>) config.getOrDefault("driverConfig", Collections.emptyMap()));
        this.localParams = (Map<String, Object>) config.getOrDefault("localParams", Collections.emptyMap());
        this.remoteParams = (Map<String, Object>) config.getOrDefault("remoteParams", Collections.emptyMap());
    }

    private Config toDriverConfig(Map<String, Object> driverConfMap) {
        String logging = (String) driverConfMap.getOrDefault("logging", "INFO");
        boolean encryption = (boolean) driverConfMap.getOrDefault("encryption", false);
        boolean logLeakedSessions = (boolean) driverConfMap.getOrDefault("logLeakedSessions", true);
        Long idleTimeBeforeConnectionTest = (Long) driverConfMap.getOrDefault("idleTimeBeforeConnectionTest", -1L);
        String trustStrategy = (String) driverConfMap.getOrDefault("trustStrategy", "TRUST_ALL_CERTIFICATES");
        Long connectionTimeoutMillis = (Long) driverConfMap.get("connectionTimeoutMillis");
        Long maxRetryTimeMs = (Long) driverConfMap.get("maxRetryTimeMs");
        Long maxConnectionLifeTime = (Long) driverConfMap.get("maxConnectionLifeTime");
        Long maxConnectionPoolSize = (Long) driverConfMap.get("maxConnectionPoolSize");
        Long routingTablePurgeDelay = (Long) driverConfMap.get("routingTablePurgeDelay");
        Long connectionAcquisitionTimeout = (Long) driverConfMap.get("connectionAcquisitionTimeout");

        Config.ConfigBuilder config = Config.builder();

        config.withLogging(new JULogging(Level.parse(logging)));
        if(encryption) config.withEncryption();
        config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        if(!logLeakedSessions) config.withoutEncryption();

        if (connectionAcquisitionTimeout!=null) {
            config.withConnectionAcquisitionTimeout(connectionAcquisitionTimeout, TimeUnit.MILLISECONDS);
        }
        if (maxConnectionLifeTime!=null) {
            config.withMaxConnectionLifetime(maxConnectionLifeTime, TimeUnit.MILLISECONDS);
        }
        if (maxConnectionPoolSize!=null) {
            config.withMaxConnectionPoolSize(maxConnectionPoolSize.intValue());
        }
        if (routingTablePurgeDelay!=null) {
            config.withRoutingTablePurgeDelay(routingTablePurgeDelay, TimeUnit.MILLISECONDS);
        }
        if (idleTimeBeforeConnectionTest!=null) {
            config.withConnectionLivenessCheckTimeout(idleTimeBeforeConnectionTest, TimeUnit.MILLISECONDS);
        }
        if (connectionTimeoutMillis!=null) {
            config.withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        if (maxRetryTimeMs!=null) {
            config.withMaxTransactionRetryTime(maxRetryTimeMs, TimeUnit.MILLISECONDS);
        }
        if(trustStrategy.equals("TRUST_ALL_CERTIFICATES")) config.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        else if(trustStrategy.equals("TRUST_SYSTEM_CA_SIGNED_CERTIFICATES")) config.withTrustStrategy(Config.TrustStrategy.trustSystemCertificates());
        else {
            File file = new File(trustStrategy);
            config.withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(file));
        }
        return config.build();
    }

    public SessionConfig getSessionConfig() {
        return SessionConfig.builder()
                .withDatabase(this.databaseName)
                .withDefaultAccessMode(readOnly ? AccessMode.READ : AccessMode.WRITE)
                .build();
    }

    public boolean isVirtual() {
        return virtual;
    }

    public boolean isAddStatistics() {
        return addStatistics;
    }


    public boolean isStreamStatements() {
        return streamStatements;
    }

    public Config getDriverConfig() {
        return driverConfig;
    }

    public Map<String, Object> getLocalParams() {
        return localParams;
    }

    public Map<String, Object> getRemoteParams() {
        return remoteParams;
    }
}