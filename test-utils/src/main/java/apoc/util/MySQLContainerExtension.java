package apoc.util;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.time.Duration;

public class MySQLContainerExtension extends MySQLContainer<MySQLContainerExtension> {

    public MySQLContainerExtension() {
        super("mysql:5.7");
        this.withInitScript("init_mysql.sql");
        this.withUrlParam("user", "test");
        this.withUrlParam("password", "test");
        this.withUrlParam("useSSL", "false");

        setWaitStrategy(new LogMessageWaitStrategy()
                .withRegEx(".*ready for connections.")
                .withStartupTimeout(Duration.ofMinutes(2)));

        addExposedPort(3306);
    }
}
