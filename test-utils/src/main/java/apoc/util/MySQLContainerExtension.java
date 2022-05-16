package apoc.util;

import org.testcontainers.containers.MySQLContainer;

public class MySQLContainerExtension extends MySQLContainer<MySQLContainerExtension> {

    public MySQLContainerExtension() {
        super("mysql:5.7");
        this.withInitScript("init_mysql.sql");
        this.withUrlParam("user", "test");
        this.withUrlParam("password", "test");
        this.withUrlParam("useSSL", "false");
    };
}
