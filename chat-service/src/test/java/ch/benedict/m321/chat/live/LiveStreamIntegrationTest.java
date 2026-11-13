package ch.benedict.m321.chat.live;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import ch.benedict.m321.chat.message.Message;
import ch.benedict.m321.chat.message.NewMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Prueft SSE ueber einen echten HTTP-Server; POST laeuft zusaetzlich durch echtes RabbitMQ. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LiveStreamIntegrationTest {
    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4-management-alpine");
    @LocalServerPort
    private int port;
    @Autowired
    private LiveMessageBuffer buffer;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    @Qualifier("liveStreamExecutor")
    private ThreadPoolTaskExecutor executor;
    private final List<SseTestClient> clients = new ArrayList<>();

    /** Isoliert den Test von lokalen Broker-Zugangsdaten; PostgreSQL wird fuer diesen Weg nicht benutzt. */
    @DynamicPropertySource
    static void connections(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", RABBIT::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBIT::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBIT::getAdminPassword);
    }

    /** Keine Stream-Verbindung und kein Sender darf in den naechsten Test hineinlaufen. */
    @AfterEach
    void cleanConnections() throws Exception {
        for (SseTestClient client : clients) {
            client.close();
        }
        buffer.clear();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(buffer.connectionCount()).isZero();
            assertThat(executor.getActiveCount()).isZero();
            assertThat(executor.getQueueSize()).isZero();
        });
    }

    /** Zwei Clients im selben Raum bekommen das echte POST-Ergebnis; ein fremder Raum bleibt leer. */
    @Test
    void postTravelsThroughBrokerToBothRoomClientsWithCorrectSseFormat() throws Exception {
        UUID room = UUID.randomUUID();
        SseTestClient first = open(room);
        SseTestClient second = open(room);
        SseTestClient otherRoom = open(UUID.randomUUID());
        assertThat(first.response().headers().firstValue("content-type").orElse("")).startsWith("text/event-stream");
        assertThat(first.response().headers().firstValue("cache-control").orElse("")).isEqualTo("no-cache");
        List<Message> expected = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            expected.add(post(room, "Hallo \"Liam\"\nGrüsse " + index));
        }
        for (Message message : expected) {
            assertEvent(first.next(Duration.ofSeconds(5)), message);
            assertEvent(second.next(Duration.ofSeconds(5)), message);
        }
        assertThat(otherRoom.next(Duration.ofMillis(300))).isNull();
        assertThat(first.next(Duration.ofMillis(300))).isNull();
        assertThat(second.next(Duration.ofMillis(300))).isNull();
    }

    /** Ohne Cursor werden alte Nachrichten nicht ungefragt wiederholt. */
    @Test
    void newConnectionReceivesOnlyMessagesAfterRegistration() throws Exception {
        UUID room = UUID.randomUUID();
        Message old = post(room, "Vor der Verbindung");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(buffer.snapshot()).contains(old));
        SseTestClient client = open(room);
        assertThat(client.next(Duration.ofMillis(200))).isNull();
        Message current = post(room, "Nach der Verbindung");
        assertEvent(client.next(Duration.ofSeconds(5)), current);
        assertThat(client.next(Duration.ofMillis(200))).isNull();
    }

    /** Ungueltige Raeume und zu viele Verbindungen scheitern vor einer erfolgreichen SSE-Antwort. */
    @Test
    void rejectsMissingRoomInvalidRoomAndConnectionOverflow() throws Exception {
        try (HttpClient http = HttpClient.newHttpClient()) {
            assertThat(get(http, "/stream").statusCode()).isEqualTo(400);
            assertThat(get(http, "/stream?roomId=kaputt").statusCode()).isEqualTo(400);
            for (int index = 0; index < 100; index++) {
                buffer.register(UUID.randomUUID());
            }
            assertThat(get(http, "/stream?roomId=" + UUID.randomUUID()).statusCode()).isEqualTo(503);
            assertThat(buffer.connectionCount()).isEqualTo(100);
        }
    }

    /** Heartbeat-Schreibversuche erkennen einen abgebrochenen Client auch ohne neue Chatnachrichten. */
    @Test
    void disconnectedClientIsRemovedAndSenderTerminates() throws Exception {
        SseTestClient client = open(UUID.randomUUID());
        assertThat(buffer.connectionCount()).isEqualTo(1);
        client.close();
        clients.remove(client);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(buffer.connectionCount()).isZero();
            assertThat(executor.getActiveCount()).isZero();
        });
    }

    /** Ein wirklich nicht lesender TCP-Client blockiert nur seinen Sender bis zum Socket-Timeout. */
    @Test
    void stalledSocketDoesNotBlockHealthyClientAndTimesOut() throws Exception {
        UUID slowRoom = UUID.randomUUID();
        UUID healthyRoom = UUID.randomUUID();
        try (Socket slow = new Socket()) {
            slow.setReceiveBufferSize(1024);
            slow.setSoTimeout(5000);
            slow.connect(new InetSocketAddress("localhost", port), 5000);
            String request = "GET /stream?roomId=" + slowRoom + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
            slow.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
            slow.getOutputStream().flush();
            BufferedReader headers = new BufferedReader(new InputStreamReader(slow.getInputStream(), StandardCharsets.UTF_8));
            assertThat(headers.readLine()).contains("200");
            String header;
            while ((header = headers.readLine()) != null && !header.isEmpty()) {
                // Nach den Headern wird absichtlich kein weiteres Byte gelesen.
            }
            SseTestClient healthy = open(healthyRoom);
            String bigText = "x".repeat(256 * 1024);
            for (int index = 0; index < 64; index++) {
                buffer.accept(new Message(UUID.randomUUID(), slowRoom, "student", "Liam", bigText, Instant.now()));
            }
            Message message = new Message(UUID.randomUUID(), healthyRoom, "student", "Liam", "Weiter erreichbar", Instant.now());
            buffer.accept(message);
            assertEvent(healthy.next(Duration.ofSeconds(3)), message);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(buffer.connectionCount()).isEqualTo(1);
                assertThat(executor.getActiveCount()).isEqualTo(1);
            });
        }
    }

    /** Oeffnet einen Stream und wartet auf dessen ersten geflushten Kommentar. */
    private SseTestClient open(UUID room) throws Exception {
        SseTestClient client = new SseTestClient(address("/stream?roomId=" + room));
        clients.add(client);
        assertThat(client.response().statusCode()).isEqualTo(200);
        assertThat(client.awaitOpened()).isTrue();
        return client;
    }

    /** Vergleicht ID und kompletten JSON-Inhalt, nicht bloss die Anzahl gelesener Ereignisse. */
    private void assertEvent(SseTestClient.Event event, Message expected) throws Exception {
        assertThat(event).isNotNull();
        assertThat(event.id()).isEqualTo(expected.id().toString());
        assertThat(objectMapper.readValue(event.data(), Message.class)).isEqualTo(expected);
    }

    /** Sendet wie ein echter Client; die Antwort liefert die serverseitige UUID und Sendezeit. */
    private Message post(UUID room, String text) throws Exception {
        NewMessage message = new NewMessage(room, "student", "Liam", text);
        String json = objectMapper.writeValueAsString(message);
        HttpRequest request = HttpRequest.newBuilder(URI.create(address("/messages")))
                .header("Content-Type", "application/json").timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(json)).build();
        try (HttpClient http = HttpClient.newHttpClient()) {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(202);
            return objectMapper.readValue(response.body(), Message.class);
        }
    }

    /** Begrenzte Fehlerantworten lassen sich ohne dauerhaften SSE-Leser pruefen. */
    private HttpResponse<String> get(HttpClient http, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(address(path)))
                .timeout(Duration.ofSeconds(5)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** Der zufaellige Testport vermeidet Konflikte mit einem laufenden Compose-Stack. */
    private String address(String path) {
        return "http://localhost:" + port + path;
    }
}
