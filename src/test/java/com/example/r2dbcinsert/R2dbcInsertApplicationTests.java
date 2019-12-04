package com.example.r2dbcinsert;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ContextConfiguration(initializers = R2dbcInsertApplicationTests.TestDatabaseConfiguration.class)
@SpringBootTest
class R2dbcInsertApplicationTests {

	@Resource
	private CustomerRepository customerRepository;

	@Resource
	private DatabaseClient databaseClient;

	@Resource
	private ConnectionFactory connectionFactory;

	@BeforeEach
	void setup() {
		customerRepository.deleteAll().block();
		assertEquals(0L, customerRepository.count().block());
	}

	@Test
	void testInsertRepository() {
		Customer customer = new Customer();
		customer.setId(1L);
		customer.setFirstName("John");
		assertEquals(customer, customerRepository.save(customer).block());
		assertEquals(1L, customerRepository.count().block());
	}

	@Test
	void testInsertDatabaseClient() {
		Customer customer = new Customer();
		customer.setId(2L);
		customer.setFirstName("John");
		databaseClient.insert().into(Customer.class).using(customer).fetch().first().block();
		assertEquals(1L, customerRepository.count().block());
	}

	@Test
	void testInsertConnectionFactory() {
		((PostgresqlConnectionFactory) ((ConnectionPool) connectionFactory).unwrap()).create()
				.flatMapMany(it ->
						it.createStatement("insert into customer (id, first_name) values (3, 'John')")
								.execute()
								.thenMany(it.close())
				).blockFirst();
		assertEquals(1L, customerRepository.count().block());
	}

	@TestConfiguration
	static class TestDatabaseConfiguration implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		private static PostgreSQLContainer postgresql;

		@Override
		public void initialize(ConfigurableApplicationContext applicationContext) {
			try {
				postgresql = new PostgreSQLContainer<>("postgres:11.3-alpine");
				postgresql.start();

				TestPropertyValues
						.of("spring.r2dbc.name=" + postgresql.getDatabaseName())
						.and("spring.r2dbc.password=" + postgresql.getPassword())
						.and("spring.r2dbc.username=" + postgresql.getUsername())
						.and("spring.r2dbc.url=" + postgresql.getJdbcUrl()
								.replace("jdbc:", "r2dbc:"))
						.applyTo(applicationContext);
			} catch (Throwable ex) {
			}
		}

		@PreDestroy
		public void stop() {
			if (postgresql != null && postgresql.isRunning()) {
				postgresql.stop();
			}
		}
	}
}
