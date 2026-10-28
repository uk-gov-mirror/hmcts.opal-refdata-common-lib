package uk.gov.hmcts.opal.refdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import uk.gov.hmcts.opal.refdata.config.RefDataServiceBusSessionReceiverConfig;
import uk.gov.hmcts.opal.refdata.fixture.DummyPrison;
import uk.gov.hmcts.opal.refdata.fixture.DummyPrisonRepository;

@SpringBootTest(
    classes = RefDataMessagingIntegrationTest.TestConfig.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.sql.init.mode=always",
        "spring.sql.init.schema-locations=classpath:ref-data-schema.sql",
        "spring.jpa.hibernate.ddl-auto=validate",
        "opal.ref-data.service-bus.consumer-enabled=true",
        "opal.ref-data.service-bus.max-messages-per-session=50",
        "opal.ref-data.service-bus.initial-delay-seconds=300",
        "opal.ref-data.service-bus.fixed-delay-seconds=300",
        "opal.ref-data.service-bus.receive-timeout-seconds=3",
        "opal.ref-data.service-bus.try-timeout-seconds=5",
        "opal.ref-data.service-bus.topic-name=opal-reference-data-topic",
        "opal.ref-data.service-bus.subscription-name=opal-fines-reference-data-subscription"
    }
)
@ActiveProfiles("integration")
@Testcontainers
class RefDataMessagingIntegrationTest {

    private static final String SESSION_ID = "ref-data-session-id";
    private static final String SQL_PASSWORD = "RefDataTests!12345";
    private static final Network NETWORK = Network.newNetwork();

    @Container
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final GenericContainer<?> SQL_SERVER = new GenericContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
        .withNetwork(NETWORK)
        .withNetworkAliases("refdata-sql")
        .withEnv("ACCEPT_EULA", "Y")
        .withEnv("MSSQL_SA_PASSWORD", SQL_PASSWORD)
        .waitingFor(Wait.forLogMessage("(?s).*SQL Server is now ready for client connections.*", 1))
        .withStartupTimeout(Duration.ofMinutes(2));

    @Container
    static final GenericContainer<?> SERVICE_BUS =
        new GenericContainer<>("mcr.microsoft.com/azure-messaging/servicebus-emulator:1.1.2")
            .withNetwork(NETWORK)
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("SQL_SERVER", "refdata-sql")
            .withEnv("MSSQL_SA_PASSWORD", SQL_PASSWORD)
            .withCopyFileToContainer(MountableFile.forClasspathResource("service-bus-config.json"),
                "/ServiceBus_Emulator/ConfigFiles/Config.json")
            .withExposedPorts(5672, 5300)
            .dependsOn(SQL_SERVER)
            .waitingFor(Wait.forHttp("/health").forPort(5300))
            .withStartupTimeout(Duration.ofMinutes(2));

    @Autowired
    private DummyPrisonRepository repository;

    @Autowired
    private RefDataServiceBusSessionPuller puller;

    private EmulatorUtil emulator;

    @DynamicPropertySource
    static void serviceBusProperties(DynamicPropertyRegistry registry) {
        registry.add("opal.common.service-bus.connection-string", RefDataMessagingIntegrationTest::connectionString);
    }

    private static String connectionString() {
        return "Endpoint=sb://" + SERVICE_BUS.getHost() + ":" + SERVICE_BUS.getMappedPort(5672)
            + ";SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;"
            + "UseDevelopmentEmulator=true;";
    }

    @BeforeEach
    void setUp() {
        emulator = new EmulatorUtil(connectionString());
        repository.deleteAll();
        DummyPrison seed = new DummyPrison();
        seed.setId(1L);
        seed.setName("Original Prison");
        seed.setAddressLine1("1 Old Road");
        seed.setPostcode("OL1 1AA");
        repository.save(seed);
    }

    @AfterEach
    void cleanUpMessages() {
        try (EmulatorUtil client = emulator) {
            client.clearSessionMessages(SESSION_ID);
        }
    }

    @AfterAll
    static void closeNetwork() {
        SERVICE_BUS.stop();
        SQL_SERVER.stop();
        NETWORK.close();
    }

    @Test
    void processMessage_updatesExistingRefDataFromPayload() {
        String payload = message(1L, "Original Prison", "2 New Road", "NW1 1AA");
        publishAndWait(payload);

        puller.pullSessionMessages();

        DummyPrison updated = repository.findByName("Original Prison").orElseThrow();
        assertThat(updated.getAddressLine1()).isEqualTo("2 New Road");
        assertThat(updated.getPostcode()).isEqualTo("NW1 1AA");
        assertThat(repository.count()).isEqualTo(1L);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(emulator.peekQueueMessages()).isEmpty();
            assertThat(emulator.peekDlqMessages()).isEmpty();
        });
    }

    @Test
    void processMessage_completesMessageWhenNoHandlerExistsForDataProduct() {
        String payload = message(1L, "MajorCreditor", "Ignored Prison", "2 New Road", "NW1 1AA");
        publishAndWait(payload);

        puller.pullSessionMessages();

        assertThat(repository.findById(1L).orElseThrow().getName()).isEqualTo("Original Prison");
        assertThat(repository.count()).isEqualTo(1L);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(emulator.peekQueueMessages()).isEmpty();
            assertThat(emulator.peekDlqMessages()).isEmpty();
        });
    }

    @Test
    void processMessage_leavesInvalidPayloadOnQueueAndNotInDlq() {
        String payload = "{xx}";
        publishAndWait(payload);

        assertThatThrownBy(puller::pullSessionMessages)
            .isInstanceOf(IllegalArgumentException.class);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(emulator.peekQueueMessages()).containsExactly(payload);
            assertThat(emulator.peekDlqMessages()).isEmpty();
        });
        assertThat(repository.findById(1L).orElseThrow().getName()).isEqualTo("Original Prison");
        assertThat(repository.count()).isEqualTo(1L);
    }

    @Test
    void processMessage_stopsProcessingSessionWhenMiddlePayloadIsInvalid() {
        String processedPayload = message(1L, "Original Prison", "2 New Road", "NW1 1AA");
        String invalidPayload = "{xx}";
        String unprocessedPayload = message(2L, "Unprocessed Prison", "3 New Road", "NW1 2AA");
        publishAndWait(processedPayload, invalidPayload, unprocessedPayload);

        assertThatThrownBy(puller::pullSessionMessages)
            .isInstanceOf(IllegalArgumentException.class);

        assertThat(repository.findById(1L).orElseThrow().getName())
            .isEqualTo("Original Prison");
        assertThat(repository.findByName("Unprocessed Prison")).isEmpty();
        assertThat(repository.count()).isEqualTo(1L);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(emulator.peekQueueMessages()).containsExactly(invalidPayload, unprocessedPayload);
            assertThat(emulator.peekDlqMessages()).isEmpty();
        });
    }

    private void publishAndWait(String... payloads) {
        for (String payload : payloads) {
            emulator.publishMessageToTopic(payload, SESSION_ID);
        }
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
            assertThat(emulator.peekQueueMessages()).containsExactly(payloads));
    }

    private static String message(long id, String name, String addressLine1, String postcode) {
        return message(id, "Prison", name, addressLine1, postcode);
    }

    private static String message(long id, String dataProduct, String name, String addressLine1, String postcode) {
        return """
            {
              "header": {
                "message_id": "00000000-0000-0000-0000-%012d",
                "message_type": "ReferenceData",
                "data_product": "%s",
                "operation": "PUBLISH",
                "source_system": "Semarchy",
                "created_date_time": "2026-01-01T00:00:00Z",
                "release_package_id": 1,
                "record_count": 1
              },
              "payload": {
                "records": [{
                  "description": "%s",
                  "publishing_status": "Active",
                  "start_date": "2026-01-01",
                  "addresses": [{
                    "address_type": "PRIS",
                    "address_line_1": "%s",
                    "post_code": "%s"
                  }]
                }]
              }
            }
        """.formatted(id, dataProduct, name, addressLine1, postcode);
    }

    @Import({RefDataMessageProcessorImplIT.TestConfig.class, RefDataServiceBusSessionReceiverConfig.class,
        RefDataServiceBusSessionPuller.class})
    static class TestConfig {
    }
}
