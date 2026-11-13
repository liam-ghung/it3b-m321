package ch.benedict.m321.chat.live;

import ch.benedict.m321.chat.ChatServiceApplication;
import ch.benedict.m321.chat.message.Message;
import ch.benedict.m321.chat.message.NewMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/** Prueft S3 mit zwei vollstaendigen HTTP-Anwendungen und einem echten gemeinsamen Broker. */
@Testcontainers
class LiveMultiInstanceTest {
    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4-management-alpine");

    /** Nach zehn Sekunden kann auch die andere Instanz den gemeinsamen Cursor ohne Datenbank nachliefern. */
    @Test
    void reconnectOnOtherInstanceReplays50ThenDeliversLive() throws Exception {
        try (var first = startInstance(); var second = startInstance()) {
            UUID room = UUID.randomUUID();
            ObjectMapper mapper = first.getBean(ObjectMapper.class);
            Message anchor;
            try (SseTestClient original = new SseTestClient(address(first) + "/stream?roomId=" + room)) {
                assertThat(original.awaitOpened()).isTrue();
                List<Message> sent = send(address(first), room, "anchor", mapper, 1);
                verify(original, sent, mapper);
                anchor = sent.get(0);
            }
            List<Message> missed = send(address(first), room, "missed", mapper, 50);
            Thread.sleep(10000);
            try (SseTestClient resumed = new SseTestClient(address(second) + "/stream?roomId=" + room,
                    anchor.id().toString())) {
                assertThat(resumed.response().statusCode()).isEqualTo(200);
                assertThat(resumed.awaitOpened()).isTrue();
                verify(resumed, missed, mapper);
                List<Message> live = send(address(first), room, "live", mapper, 1);
                verify(resumed, live, mapper);
            }
        }
    }

    /** Zwei unabhaengige Absender senden parallel; ihre jeweils 200 POSTs bleiben sequenziell. */
    @Test
    void bothInstancesDeliverAll400MessagesOnceInSenderOrder() throws Exception {
        try (var first = startInstance(); var second = startInstance()) {
            String firstAddress = address(first);
            String secondAddress = address(second);
            ObjectMapper mapper = first.getBean(ObjectMapper.class);
            UUID room = UUID.randomUUID();
            try (SseTestClient firstClient = new SseTestClient(firstAddress + "/stream?roomId=" + room);
                    SseTestClient secondClient = new SseTestClient(secondAddress + "/stream?roomId=" + room);
                    SseTestClient otherRoom = new SseTestClient(secondAddress + "/stream?roomId=" + UUID.randomUUID());
                    var senders = Executors.newFixedThreadPool(2)) {
                assertThat(firstClient.awaitOpened()).isTrue();
                assertThat(secondClient.awaitOpened()).isTrue();
                assertThat(otherRoom.awaitOpened()).isTrue();
                var firstSent = senders.submit(() -> send(firstAddress, room, "sender-a", mapper));
                var secondSent = senders.submit(() -> send(secondAddress, room, "sender-b", mapper));
                List<Message> expected = new ArrayList<>(firstSent.get(60, TimeUnit.SECONDS));
                expected.addAll(secondSent.get(60, TimeUnit.SECONDS));
                verify(firstClient, expected, mapper);
                verify(secondClient, expected, mapper);
                assertThat(otherRoom.next(Duration.ofMillis(500))).isNull();
            }
        }
    }

    /** Jeder Kontext hat eigenen HTTP-Port, Consumer, Live-Puffer und Client-Verwaltung. */
    private ServletWebServerApplicationContext startInstance() {
        SpringApplication app = new SpringApplication(ChatServiceApplication.class);
        return (ServletWebServerApplicationContext) app.run(
                "--server.port=0", "--spring.main.banner-mode=off",
                "--spring.datasource.url=jdbc:postgresql://127.0.0.1:1/unavailable",
                "--spring.rabbitmq.host=" + RABBIT.getHost(),
                "--spring.rabbitmq.port=" + RABBIT.getAmqpPort(),
                "--spring.rabbitmq.username=" + RABBIT.getAdminUsername(),
                "--spring.rabbitmq.password=" + RABBIT.getAdminPassword(),
                "--logging.level.ch.benedict.m321=INFO");
    }

    /** Spricht die konkrete Instanz an, damit kein Loadbalancer den Test verfaelschen kann. */
    private String address(ServletWebServerApplicationContext context) {
        return "http://localhost:" + context.getWebServer().getPort();
    }

    /** Die Serverantwort liefert die erwartete ID und den kompletten Nachrichteninhalt. */
    private List<Message> send(String address, UUID room, String sender, ObjectMapper mapper) throws Exception {
        return send(address, room, sender, mapper, 200);
    }

    /** Verwendet fuer Replay dieselbe echte POST-Strecke mit einer kleineren vorgegebenen Nachrichtenanzahl. */
    private List<Message> send(String address, UUID room, String sender, ObjectMapper mapper, int count) throws Exception {
        List<Message> messages = new ArrayList<>();
        try (HttpClient http = HttpClient.newHttpClient()) {
            for (int index = 0; index < count; index++) {
                NewMessage message = new NewMessage(room, sender, sender, "Nachricht " + index);
                String json = mapper.writeValueAsString(message);
                HttpRequest request = HttpRequest.newBuilder(URI.create(address + "/messages"))
                        .header("Content-Type", "application/json").timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(json)).build();
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(202);
                messages.add(mapper.readValue(response.body(), Message.class));
            }
        }
        return messages;
    }

    /** Vergleicht jedes JSON und jede ID sowie die Reihenfolge je Absender, ohne globale Ordnung zu erfinden. */
    private void verify(SseTestClient client, List<Message> expected, ObjectMapper mapper) throws Exception {
        Map<UUID, Message> remaining = new HashMap<>();
        Map<String, List<UUID>> expectedOrder = new HashMap<>();
        Map<String, List<UUID>> actualOrder = new HashMap<>();
        for (Message message : expected) {
            assertThat(remaining.put(message.id(), message)).isNull();
            expectedOrder.computeIfAbsent(message.senderId(), key -> new ArrayList<>()).add(message.id());
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        for (int index = 0; index < expected.size(); index++) {
            long left = deadline - System.nanoTime();
            assertThat(left).isPositive();
            SseTestClient.Event event = client.next(Duration.ofNanos(left));
            assertThat(event).isNotNull();
            Message actual = mapper.readValue(event.data(), Message.class);
            assertThat(event.id()).isEqualTo(actual.id().toString());
            Message wanted = remaining.remove(actual.id());
            assertThat(wanted).as("Unbekannte oder doppelte ID").isNotNull();
            assertThat(actual).isEqualTo(wanted);
            actualOrder.computeIfAbsent(actual.senderId(), key -> new ArrayList<>()).add(actual.id());
        }
        assertThat(remaining).isEmpty();
        assertThat(actualOrder).isEqualTo(expectedOrder);
        assertThat(client.next(Duration.ofMillis(500))).isNull();
    }
}
