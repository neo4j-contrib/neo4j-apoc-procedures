package apoc.util;

import org.jetbrains.annotations.NotNull;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SocatContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

/*
 * Thanks to Michael Simons that inspired this.
 *
 * https://github.com/michael-simons/junit-jupiter-causal-cluster-testcontainer-extension/blob/master/src/main/java/org/neo4j/junit/jupiter/causal_cluster/CausalCluster.java
 */

public class TestcontainersCausalCluster {
    private static final int DEFAULT_BOLT_PORT = 7687;

    public enum ClusterInstanceType {CORE, READ_REPLICA}

    private static Stream<AbstractMap.SimpleEntry<Integer, String>> iterateMembers(int numOfMembers, ClusterInstanceType instanceType) {
        final IntFunction<String> generateInstanceName = i -> String.format("neo4j-%s-%d", instanceType.toString(), i);

        return IntStream.rangeClosed(1, numOfMembers).mapToObj(i -> new AbstractMap.SimpleEntry<>(i - 1, generateInstanceName.apply(i)));
    }

    private static Stream<AbstractMap.SimpleEntry<Integer, String>> iterateReplicaMembers(int numOfMembers, int numCoreMembers, ClusterInstanceType instanceType) {
        final IntFunction<String> generateInstanceName = i -> String.format("neo4j-%s-%d", instanceType.toString(), i);

        return IntStream.rangeClosed(numCoreMembers + 1, numOfMembers).mapToObj(i -> new AbstractMap.SimpleEntry<>(i - 1, generateInstanceName.apply(i)));
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

        // Prepare proxy to enter the cluster
        final SocatContainer proxy = new SocatContainer().withNetwork(network);
        iterateMembers(numberOfCoreMembers, ClusterInstanceType.CORE)
                .forEach(member -> proxy.withTarget(DEFAULT_BOLT_PORT + member.getKey(), member.getValue(), DEFAULT_BOLT_PORT));

        // Start the proxy so that the exposed ports are available and we can get the mapped ones
        proxy.start();

        // Build the core/read_replica
        List<Neo4jContainerExtension> members = getClusterMembers(numberOfCoreMembers, ClusterInstanceType.CORE, proxy, network, initialDiscoveryMembers, neo4jConfig, envSettings, timeout);

        ClusterInstanceType instanceType = ClusterInstanceType.READ_REPLICA;
        WaitStrategy waitForBolt = new LogMessageWaitStrategy()
                .withRegEx(String.format(".*Bolt enabled on 0\\.0\\.0\\.0:%d\\.\n", DEFAULT_BOLT_PORT))
                .withStartupTimeout(timeout);
        Function<GenericContainer, String> getProxyUrl = instance -> String.format("%s:%d", instance.getContainerIpAddress(), instance.getMappedPort(DEFAULT_BOLT_PORT));
        members.addAll(iterateReplicaMembers(numberOfReadReplica, numberOfCoreMembers, instanceType)
                .map(member -> getNeo4jContainerExtension(waitForBolt, network, initialDiscoveryMembers, proxy, getProxyUrl, instanceType, neo4jConfig, envSettings, member))
                .collect(toList()));

        // Start all of them in parallel
        final CountDownLatch latch = new CountDownLatch(numberOfCoreMembers);
        members.forEach(instance -> CompletableFuture.runAsync(() -> {
            instance.start();
            latch.countDown();
        }));

        try {
            latch.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new TestcontainersCausalCluster(members, proxy);
    }

    @NotNull
    private static List<Neo4jContainerExtension> getClusterMembers(int numberOfCoreMembers,
                                                                   ClusterInstanceType instanceType,
                                                                   SocatContainer proxy,
                                                                   Network network,
                                                                   String initialDiscoveryMembers,
                                                                   Map<String, Object> neo4jConfig,
                                                                   Map<String, String> envSettings, Duration timeout) {
        // Currently needed as a whole new waiting strategy due to a bug in test containers
        WaitStrategy waitForBolt = new LogMessageWaitStrategy()
                .withRegEx(String.format(".*Bolt enabled on 0\\.0\\.0\\.0:%d\\.\n", DEFAULT_BOLT_PORT))
                .withStartupTimeout(timeout);
        Function<GenericContainer, String> getProxyUrl = instance ->
                String.format("%s:%d", instance.getContainerIpAddress(), instance.getMappedPort(DEFAULT_BOLT_PORT));
        return iterateMembers(numberOfCoreMembers, instanceType)
                .map(member -> getNeo4jContainerExtension(waitForBolt, network, initialDiscoveryMembers, proxy, getProxyUrl, instanceType, neo4jConfig, envSettings, member))
                .collect(toList());
    }

//    @NotNull
//    private static Map<String, GenericContainer> createSidecars(int numOfMembers, Network network, ClusterInstanceType instanceType) {
//        return iterateMembers(numOfMembers, instanceType)
//                .collect(toMap(
//                        Function.identity(),
//                        name -> new GenericContainer("alpine/socat")
//                                .withNetwork(network)
//                                .withLabel("memberType", instanceType.toString())
//                                // Expose the default bolt port on the sidecar
//                                .withExposedPorts(DEFAULT_BOLT_PORT)
//                                // And redirect that port to the corresponding Neo4j instance
//                                .withCommand(String
//                                        .format("tcp-listen:%d,fork,reuseaddr tcp-connect:%s:%1$d", DEFAULT_BOLT_PORT, name))
//                ));
//    }

    private static Neo4jContainerExtension getNeo4jContainerExtension(WaitStrategy waitForBolt,
                                                                      Network network,
                                                                      String initialDiscoveryMembers,
                                                                      SocatContainer proxy,
                                                                      Function<GenericContainer, String> getProxyUrl,
                                                                      ClusterInstanceType instanceType,
                                                                      Map<String, Object> neo4jConfig,
                                                                      Map<String, String> envSettings, AbstractMap.SimpleEntry<Integer, String> member) {
        Neo4jContainerExtension container =  TestContainerUtil.createEnterpriseDB(!TestUtil.isTravis())
                .withLabel("memberType", instanceType.toString())
                .withNetwork(network)
                .withNetworkAliases(member.getValue())
                .withCreateContainerCmdModifier(cmd -> {cmd.withHostName(member.getValue()); cmd.withMemory(512 * 1024 * 1024l   );})
                .withoutDriver()
                .withNeo4jConfig("dbms.mode", instanceType.toString())
                .withNeo4jConfig("dbms.default_listen_address", "0.0.0.0")
                .withNeo4jConfig("dbms.default_advertised_address", member.getValue())
                .withNeo4jConfig("dbms.connector.bolt.advertised_address", String.format("%s:%d", proxy.getContainerIpAddress(), proxy.getMappedPort(DEFAULT_BOLT_PORT + member.getKey())))
                .withNeo4jConfig("causal_clustering.initial_discovery_members", initialDiscoveryMembers)
                .waitingFor(waitForBolt);
        neo4jConfig.forEach((conf, value) -> container.withNeo4jConfig(conf, String.valueOf(value)));
        container.withEnv(envSettings);
        return container;
    }

    private final List<Neo4jContainerExtension> clusterMembers;
//    private final List<GenericContainer> sidecars;

    private Driver driver;
    private Session session;
    private SocatContainer proxy;

    public TestcontainersCausalCluster(List<Neo4jContainerExtension> clusterMembers,
                                       SocatContainer proxy) {
        this.clusterMembers = clusterMembers;
        this.proxy = proxy;
        this.driver = GraphDatabase.driver(getURI(), AuthTokens.basic("neo4j", "apoc"));
        this.session = driver.session();
    }

    public Driver getDriver() {
        return driver;
    }

    public Session getSession() {
        return session;
    }

    public URI getURI() {
        return URI.create(String.format("neo4j://%s:%d", proxy.getContainerIpAddress(), proxy.getMappedPort(DEFAULT_BOLT_PORT)));
    }


    public void close() {
        getSession().close();
        getDriver().close();
        proxy.stop();
        clusterMembers.forEach(Neo4jContainerExtension::stop);
    }
}
