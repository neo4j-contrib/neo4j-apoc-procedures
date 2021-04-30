package apoc.util;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SocatContainer;

import java.net.URI;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/*
 * Thanks to Michael Simons that inspired this.
 *
 * https://github.com/michael-simons/junit-jupiter-causal-cluster-testcontainer-extension/blob/master/src/main/java/org/neo4j/junit/jupiter/causal_cluster/CausalCluster.java
 */

public class TestcontainersCausalCluster {
    private static int MINUTES_TO_WAIT = 5;
    private static final int DEFAULT_BOLT_PORT = 7687;

    public enum ClusterInstanceType {
        CORE(DEFAULT_BOLT_PORT), READ_REPLICA(DEFAULT_BOLT_PORT + 1000);

        private final int port;
        ClusterInstanceType(int port) {
            this.port = port;
        }
    }

    private static Stream<Map.Entry<Integer, String>> iterateMembers(int numOfMembers, ClusterInstanceType instanceType) {
        final IntFunction<String> generateInstanceName = i -> String.format("neo4j-%s-%d", instanceType.toString(), i);

        return IntStream.rangeClosed(1, numOfMembers)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(i - 1, generateInstanceName.apply(i)));
    }

    public static TestcontainersCausalCluster create(int numberOfCoreMembers, int numberOfReadReplica, Duration timeout, Map<String, Object> neo4jConfig, Map<String, String> envSettings) {
        if (numberOfCoreMembers < 3) {
            throw new IllegalArgumentException("numberOfCoreMembers must be >= 3");
        }
        if (numberOfReadReplica < 0) {
            throw new IllegalArgumentException("numberOfReadReplica must be >= 0");
        }

        // Setup a naming strategy and the initial discovery members
        final String initialDiscoveryMembers = iterateMembers(numberOfCoreMembers, ClusterInstanceType.CORE)
                .map(n -> String.format("%s:5000", n.getValue()))
                .collect(joining(","));

        // Prepare one shared network for those containers
        Network network = Network.newNetwork();

        // Prepare proxy as sidecar
        final SocatContainer proxy = new SocatContainer()
                .withNetwork(network);
        iterateMembers(numberOfCoreMembers, ClusterInstanceType.CORE)
                .forEach(member -> proxy.withTarget(ClusterInstanceType.CORE.port + member.getKey(), member.getValue(), DEFAULT_BOLT_PORT));
        iterateMembers(numberOfReadReplica, ClusterInstanceType.READ_REPLICA)
                .forEach(member -> proxy.withTarget(ClusterInstanceType.READ_REPLICA.port + member.getKey(), member.getValue(), DEFAULT_BOLT_PORT));


        proxy.start();

        // Build the core/read_replica
        List<Neo4jContainerExtension> members = iterateMembers(numberOfCoreMembers, ClusterInstanceType.CORE)
                .map(member -> createInstance(member.getValue(), ClusterInstanceType.CORE, network, initialDiscoveryMembers, neo4jConfig, envSettings)
                        .withNeo4jConfig("dbms.default_advertised_address", member.getValue())
                        .withNeo4jConfig("dbms.connector.bolt.advertised_address", String.format("%s:%d", proxy.getContainerIpAddress(), proxy.getMappedPort(ClusterInstanceType.CORE.port + member.getKey()))))
                .collect(toList());
        members.addAll(iterateMembers(numberOfReadReplica, ClusterInstanceType.READ_REPLICA)
                .map(member -> createInstance(member.getValue(), ClusterInstanceType.READ_REPLICA, network, initialDiscoveryMembers, neo4jConfig, envSettings)
                        .withNeo4jConfig("dbms.default_advertised_address", member.getValue())
                        .withNeo4jConfig("dbms.connector.bolt.advertised_address", String.format("%s:%d", proxy.getContainerIpAddress(), proxy.getMappedPort(ClusterInstanceType.READ_REPLICA.port + member.getKey()))))
                .collect(toList()));

        // Start all of them in parallel
        final CountDownLatch latch = new CountDownLatch(numberOfCoreMembers + numberOfReadReplica);
        members.forEach(instance -> CompletableFuture.runAsync(() -> {
            instance.start();
            latch.countDown();
        }));

        try {
            latch.await(MINUTES_TO_WAIT, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return new TestcontainersCausalCluster(members, proxy);
    }

    private static Neo4jContainerExtension createInstance(String name,
                                                          ClusterInstanceType instanceType,
                                                          Network network,
                                                          String initialDiscoveryMembers,
                                                          Map<String, Object> neo4jConfig,
                                                          Map<String, String> envSettings)  {
        Neo4jContainerExtension container =  TestContainerUtil.createEnterpriseDB(!TestUtil.isRunningInCI())
                .withLabel("memberType", instanceType.toString())
                .withNetwork(network)
                .withNetworkAliases(name)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(name))
                .withoutDriver()
                .withNeo4jConfig("dbms.mode", instanceType.toString())
                .withNeo4jConfig("dbms.default_listen_address", "0.0.0.0")
                .withNeo4jConfig("causal_clustering.leadership_balancing", "NO_BALANCING")
                .withNeo4jConfig("causal_clustering.initial_discovery_members", initialDiscoveryMembers)
                .withStartupTimeout(Duration.ofMinutes(MINUTES_TO_WAIT));
        neo4jConfig.forEach((conf, value) -> container.withNeo4jConfig(conf, String.valueOf(value)));
        container.withEnv(envSettings);
        return container;
    }

    private final List<Neo4jContainerExtension> clusterMembers;
    private final SocatContainer sidecar;

    private Driver driver;
    private Session session;

    public TestcontainersCausalCluster(List<Neo4jContainerExtension> clusterMembers,
                                       SocatContainer sidecars) {
        this.clusterMembers = clusterMembers;
        this.sidecar = sidecars;
        this.driver = GraphDatabase.driver(getURI(), AuthTokens.basic("neo4j", "apoc"));
        this.session = driver.session();
    }

    public List<Neo4jContainerExtension> getClusterMembers() {
        return clusterMembers;
    }

    public Driver getDriver() {
        return driver;
    }

    public Session getSession() {
        return session;
    }

    public URI getURI() {
        return Optional.of(this.sidecar)
                .map(instance -> String.format("neo4j://%s:%d", instance.getContainerIpAddress(),
                        instance.getMappedPort(DEFAULT_BOLT_PORT)))
                .map(URI::create)
                .orElseThrow(() -> new IllegalStateException("No sidecar as entrypoint into the cluster available."));
    }

    public void close() {
        getSession().close();
        getDriver().close();
        sidecar.stop();
        clusterMembers.forEach(Neo4jContainerExtension::stop);
    }

    public boolean isRunning() {
        return clusterMembers.stream().allMatch(GenericContainer::isRunning)
                && sidecar.isRunning();
    }
}
