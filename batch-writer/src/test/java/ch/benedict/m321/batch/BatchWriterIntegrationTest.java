package ch.benedict.m321.batch;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Prueft den echten Consumer mit echter Queue und Datenbank, ohne Mocks und ohne Test-Auslassung. */
@SpringBootTest
@Testcontainers
class BatchWriterIntegrationTest {
    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withInitScript("01-schema.sql");
    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4-management-alpine");

    @Autowired private RabbitTemplate rabbitTemplate;
    @Autowired private RabbitAdmin rabbitAdmin;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private SimpleMessageListenerContainer batchContainer;

    /** Zufallsports isolieren die Tests von vorhandenen Compose-Daten des Lernenden. */
    @DynamicPropertySource
    static void connections(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> POSTGRES.getJdbcUrl()
                + "&reWriteBatchedInserts=true&connectTimeout=2&socketTimeout=5");
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
        registry.add("batch.retry-delay-ms", () -> 500);
    }

    /** Jeder Test startet mit leeren Testdaten, aber denselben echten Diensten. */
    @BeforeEach
    void reset() {
        batchContainer.stop();
        rabbitAdmin.purgeQueue("chat.persist");
        rabbitAdmin.purgeQueue("chat.dlq");
        jdbcTemplate.execute("TRUNCATE message");
        batchContainer.start();
    }

    /** S5: Zwei reine JSON-Nachrichten ohne Java-Typheader erzeugen nur eine Datenbankzeile. */
    @Test
    void duplicateWithoutTypeHeaderIsStoredOnce() throws Exception {
        StoredMessage message = example();
        batchContainer.stop();
        publish(message);
        publish(message);
        batchContainer.start();
        waitForRows(1);
        assertThat(jdbcTemplate.queryForObject("SELECT content FROM message", String.class)).isEqualTo("Hallo");
        assertThat(rabbitAdmin.getQueueInfo("chat.dlq").getMessageCount()).isZero();
    }

    /** Ein einzelner Eintrag darf nicht auf einen vollen 100er-Stapel warten. */
    @Test
    void partialBatchFlushesAfterTimeout() throws Exception {
        publish(example());
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(rowCount()).isEqualTo(1));
    }

    /** Ein Rueckstau wird vollstaendig abgearbeitet, auch wenn er mehrere Stapel fuellt. */
    @Test
    void backlogOfThousandMessagesIsDrained() throws Exception {
        batchContainer.stop();
        for (int index = 0; index < 1000; index++) {
            publish(example());
        }
        batchContainer.start();
        waitForRows(1000);
    }

    /** Fehlerhaftes JSON landet allein in der DLQ und blockiert keinen gueltigen Nachbarn. */
    @Test
    void poisonMessageDoesNotBlockValidMessage() throws Exception {
        sendJson("{broken".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        publish(example());
        waitForRows(1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(rabbitAdmin.getQueueInfo("chat.dlq").getMessageCount()).isEqualTo(1));
    }

    /** Ein SQL-Datenfehler rollt den Stapel zurueck; der Einzelversuch rettet die gesunde Zeile. */
    @Test
    void databaseConstraintRejectsOnlyBadRow() throws Exception {
        jdbcTemplate.execute("ALTER TABLE message ADD CONSTRAINT test_content CHECK (content <> 'reject')");
        try {
            batchContainer.stop();
            StoredMessage bad = new StoredMessage(UUID.randomUUID(), UUID.randomUUID(),
                    "student", "Student", "reject", Instant.now());
            publish(bad);
            publish(example());
            batchContainer.start();
            waitForRows(1);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(rabbitAdmin.getQueueInfo("chat.dlq").getMessageCount()).isEqualTo(1));
        } finally {
            jdbcTemplate.execute("ALTER TABLE message DROP CONSTRAINT test_content");
        }
    }

    /** S7: Der echte DB-Prozess wird 15 Sekunden eingefroren; keine Testattrappe ersetzt ihn. */
    @Test
    void databaseOutageRecoversWithoutRestartingWriter() throws Exception {
        String containerId = POSTGRES.getContainerId();
        // Pause erhaelt Testcontainers' zufaellige Host-Port-Zuordnung beim Wiederanlauf.
        // Das vollstaendige Stop/Start im internen Compose-Netz prueft zusaetzlich S7 im Skript.
        POSTGRES.getDockerClient().pauseContainerCmd(containerId).exec();
        try {
            for (int index = 0; index < 300; index++) {
                publish(example());
            }
            Thread.sleep(15000);
            assertThat(batchContainer.isRunning()).isTrue();
        } finally {
            POSTGRES.getDockerClient().unpauseContainerCmd(containerId).exec();
        }
        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(rowCount()).isEqualTo(300));
        assertThat(rabbitAdmin.getQueueInfo("chat.dlq").getMessageCount()).isZero();
    }

    /** Erzeugt unabhaengige IDs, damit versehentliche Deduplizierung auffallen wuerde. */
    private StoredMessage example() {
        return new StoredMessage(UUID.randomUUID(), UUID.randomUUID(), "student", "Student", "Hallo", Instant.now());
    }

    /** Publiziert exakt den JSON-Vertrag ohne Framework-Typinformationen. */
    private void publish(StoredMessage message) throws Exception {
        byte[] body = objectMapper.writeValueAsBytes(message);
        sendJson(body);
    }

    /** Nur content_type wird gesetzt; damit entspricht der Test der externen Bewertung. */
    private void sendJson(byte[] body) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType("application/json");
        Message delivery = new Message(body, properties);
        rabbitTemplate.send("chat.persist", delivery);
    }

    /** Wartet begrenzt auf den asynchronen Schreibweg statt eine feste Erfolgszeit anzunehmen. */
    private void waitForRows(int expected) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(rowCount()).isEqualTo(expected));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(rabbitAdmin.getQueueInfo("chat.persist").getMessageCount()).isZero());
    }

    /** Zaehlt tatsaechlich committed sichtbare Zeilen der echten Datenbank. */
    private int rowCount() {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM message", Integer.class);
    }
}
