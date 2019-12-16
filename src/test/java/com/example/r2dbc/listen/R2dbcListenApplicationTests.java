package com.example.r2dbc.listen;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;

@ContextConfiguration(initializers = R2dbcListenApplicationTests.TestDatabaseConfiguration.class)
@SpringBootTest
class R2dbcListenApplicationTests {

    private static final PostgreSQLContainer postgresql = new PostgreSQLContainer<>("postgres:11.3-alpine");

    @Resource
    private DatabaseClient databaseClient;

    @Resource
    private ConnectionPool connectionPool;

    @Test
    @DirtiesContext
    void testListen() {
        PostgresqlConnectionFactory postgresqlConnectionFactory = (PostgresqlConnectionFactory) connectionPool.unwrap();

        Mono<?> notify = databaseClient.execute("NOTIFY message, 'hello world'")
                .fetch()
                .first()
                .log();

        postgresqlConnectionFactory.create()
                .flatMapMany(it ->
                        it.createStatement("LISTEN message").execute().thenMany(it.getNotifications())
                )
                .log()
                .as(StepVerifier::create)
                .expectNextCount(0)
                .thenAwait(Duration.ofMillis(200))
                .then(() -> notify.block())
                .expectNextMatches(n -> n.getName().equals("message") && n.getParameter().equals("hello world"))
                .thenCancel()
                .verify(Duration.ofMillis(500));
    }

    @Test
    @DirtiesContext
    void testDisconnect() {
        PostgresqlConnectionFactory postgresqlConnectionFactory = (PostgresqlConnectionFactory) connectionPool.unwrap();

        postgresqlConnectionFactory.create()
                .flatMapMany(it ->
                        it.createStatement("LISTEN message").execute().thenMany(it.getNotifications())
                )
                .log()
                .as(StepVerifier::create)
                .expectNextCount(0)
                .thenAwait(Duration.ofMillis(200))
                .then(() -> postgresql.stop())
                .expectError()
                .verify(Duration.ofSeconds(1));
    }

    @TestConfiguration
    static class TestDatabaseConfiguration implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            postgresql.start();
            TestPropertyValues
                    .of("spring.r2dbc.name=" + postgresql.getDatabaseName())
                    .and("spring.r2dbc.password=" + postgresql.getPassword())
                    .and("spring.r2dbc.username=" + postgresql.getUsername())
                    .and("spring.r2dbc.url=" + postgresql.getJdbcUrl()
                            .replace("jdbc:", "r2dbc:"))
                    .applyTo(applicationContext);
        }

        @PreDestroy
        public void stop() {
            if (postgresql.isRunning()) {
                postgresql.stop();
            }
        }
    }
}
