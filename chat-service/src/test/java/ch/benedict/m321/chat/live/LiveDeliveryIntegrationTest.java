package ch.benedict.m321.chat.live;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import ch.benedict.m321.chat.message.Message;
import ch.benedict.m321.chat.rabbit.RabbitConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Zwei echte Spring-Empfaenger teilen einen echten Broker, aber keine Queue oder Empfangsliste. */
@Testcontainers
class LiveDeliveryIntegrationTest {
    @Container
    private static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4-management-alpine");
    private final ObjectMapper inspector = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private ConfigurableApplicationContext first;
    private ConfigurableApplicationContext second;

    /** Getrennte Kontexte bilden die Broker-Seite zweier Anwendungsinstanzen ohne Webserver ab. */
    @BeforeEach
    void startInstances() {
        first = startInstance();
        second = startInstance();
        first.getBean(RabbitAdmin.class).purgeQueue("chat.persist");
        first.getBean(RabbitAdmin.class).purgeQueue("chat.dlq");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(queueInfo(queueName(first)).path("consumers").asInt()).isEqualTo(1);
            assertThat(queueInfo(queueName(second)).path("consumers").asInt()).isEqualTo(1);
        });
    }

    /** Kontexte vor dem Testcontainer schliessen, damit keine Wiederanlauf-Threads uebrig bleiben. */
    @AfterEach
    void stopInstances() {
        if (second != null) {
            second.close();
        }
        if (first != null) {
            first.close();
        }
    }

    /** Ein Publish erreicht beide Instanzen und weiterhin den bestehenden Speicherweg. */
    @Test
    void bothInstancesReceiveEveryMessageInOrderAndPersistenceKeepsItsCopy() throws Exception {
        List<Message> expected = new ArrayList<>();
        RabbitTemplate publisher = first.getBean(RabbitTemplate.class);
        for (int index = 0; index < 20; index++) {
            Message message = example("Nachricht " + index);
            expected.add(message);
            publisher.convertAndSend("chat.messages", "", message);
        }
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(first.getBean(LiveMessageBuffer.class).snapshot()).containsExactlyElementsOf(expected);
            assertThat(second.getBean(LiveMessageBuffer.class).snapshot()).containsExactlyElementsOf(expected);
            assertThat(first.getBean(RabbitAdmin.class).getQueueInfo("chat.persist").getMessageCount()).isEqualTo(20);
            assertThat(queueInfo(queueName(first)).path("messages_unacknowledged").asInt()).isZero();
            assertThat(queueInfo(queueName(second)).path("messages_unacknowledged").asInt()).isZero();
        });
        for (Message message : expected) {
            org.springframework.amqp.core.Message stored = publisher.receive("chat.persist", 3000);
            assertThat(stored).isNotNull();
            Message decoded = first.getBean(ObjectMapper.class).readValue(stored.getBody(), Message.class);
            assertThat(decoded).isEqualTo(message);
        }
    }

    /** S4 auf Broker-Ebene: genau zwei Live-Bindings, nach Instanzende nur noch eines. */
    @Test
    void closingInstanceDeletesOnlyItsExclusiveQueueWithinThirtySeconds() throws Exception {
        String firstQueue = queueName(first);
        String secondQueue = queueName(second);
        assertThat(firstQueue).isNotEqualTo(secondQueue);
        JsonNode info = queueInfo(firstQueue);
        assertThat(info.path("exclusive").asBoolean()).isTrue();
        assertThat(info.path("auto_delete").asBoolean()).isTrue();
        assertThat(info.path("durable").asBoolean()).isFalse();
        assertThat(info.path("arguments").path("x-max-length").asInt()).isEqualTo(999);
        assertThat(info.path("arguments").path("x-message-ttl").asInt()).isEqualTo(30000);
        assertThat(info.path("arguments").path("x-overflow").asText()).isEqualTo("drop-head");
        assertThat(liveBindings()).containsExactlyInAnyOrder(firstQueue, secondQueue);
        JsonNode consumers = request("GET", "/api/consumers/%2F");
        int checked = 0;
        for (JsonNode consumer : consumers) {
            String queue = consumer.path("queue").path("name").asText();
            if (queue.equals(firstQueue) || queue.equals(secondQueue)) {
                assertThat(consumer.path("prefetch_count").asInt()).isEqualTo(1);
                assertThat(consumer.path("ack_required").asBoolean()).isTrue();
                checked++;
            }
        }
        assertThat(checked).isEqualTo(2);
        second.close();
        second = null;
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(requestStatus("GET", "/api/queues/%2F/" + encode(secondQueue))).isEqualTo(404);
            assertThat(liveBindings()).containsExactly(firstQueue);
            assertThat(queueInfo(firstQueue).path("consumers").asInt()).isEqualTo(1);
            assertThat(queueInfo("chat.persist").path("durable").asBoolean()).isTrue();
            assertThat(queueInfo("chat.dlq").path("durable").asBoolean()).isTrue();
        });
    }

    /** Ungueltiges JSON wird bestaendig verworfen; eine gueltige Nachricht ohne Typheader kommt an. */
    @Test
    void malformedDeliveryDoesNotBlockPlainJsonAndDuplicateIdsAreSuppressed() throws Exception {
        RabbitTemplate publisher = first.getBean(RabbitTemplate.class);
        publisher.send("chat.delivery", "", raw("{kaputt"));
        publisher.send("chat.delivery", "", raw("null"));
        publisher.send("chat.delivery", "", raw("{}"));
        Message message = example("Nur JSON");
        String json = first.getBean(ObjectMapper.class).writeValueAsString(message);
        publisher.send("chat.delivery", "", raw(json));
        publisher.send("chat.delivery", "", raw(json));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(first.getBean(LiveMessageBuffer.class).snapshot()).containsExactly(message);
            assertThat(second.getBean(LiveMessageBuffer.class).snapshot()).containsExactly(message);
            assertThat(queueInfo(queueName(first)).path("messages").asInt()).isZero();
            assertThat(queueInfo(queueName(second)).path("messages").asInt()).isZero();
        });
    }

    /** Ein vom Broker erzwungener Verbindungsabbruch leert den Zustand und der Consumer erholt sich. */
    @Test
    void connectionLossClearsBufferAndAutomaticallyRestoresDelivery() throws Exception {
        Message before = example("Vor dem Abbruch");
        second.getBean(RabbitTemplate.class).convertAndSend("chat.messages", "", before);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(first.getBean(LiveMessageBuffer.class).snapshot()).containsExactly(before));

        CachingConnectionFactory factory = first.getBean(CachingConnectionFactory.class);
        String clientName = factory.createConnection().getDelegate().getClientProvidedName();
        String connectionName = null;
        for (JsonNode connection : request("GET", "/api/connections")) {
            String name = connection.path("client_properties").path("connection_name").asText();
            if (name.equals(clientName)) {
                connectionName = connection.path("name").asText();
            }
        }
        assertThat(connectionName).isNotNull();
        assertThat(requestStatus("DELETE", "/api/connections/" + encode(connectionName))).isEqualTo(204);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(first.getBean(LiveMessageBuffer.class).snapshot()).isEmpty();
            assertThat(queueInfo(queueName(first)).path("consumers").asInt()).isEqualTo(1);
        });
        Message after = example("Nach dem Abbruch");
        // Verifikation ueber eine neue Lieferung; ein eventuell alter Management-Zustand reicht nicht.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            second.getBean(RabbitTemplate.class).convertAndSend("chat.messages", "", after);
            assertThat(first.getBean(LiveMessageBuffer.class).snapshot()).containsExactly(after);
        });
    }

    /** Startet ausschliesslich die echten Produktionsbausteine des Broker-Empfangs. */
    private ConfigurableApplicationContext startInstance() {
        SpringApplication application = new SpringApplication(BrokerTestApplication.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        // Startargumente haben Vorrang vor application.yml und dessen localhost-Standardwerten.
        return application.run(
                "--spring.rabbitmq.host=" + RABBIT.getHost(),
                "--spring.rabbitmq.port=" + RABBIT.getAmqpPort(),
                "--spring.rabbitmq.username=" + RABBIT.getAdminUsername(),
                "--spring.rabbitmq.password=" + RABBIT.getAdminPassword(),
                "--spring.main.banner-mode=off");
    }

    /** Findet den Namen der instanzeigenen Queue ohne globale Test-Zustandsannahme. */
    private String queueName(ConfigurableApplicationContext context) {
        return context.getBean("liveQueue", Queue.class).getName();
    }

    /** Erzeugt eine echte Vertragsnachricht; Sender-ID und Name sind absichtlich verschieden. */
    private Message example(String text) {
        return new Message(UUID.randomUUID(), UUID.randomUUID(), "student-1", "Liam", text, Instant.now());
    }

    /** Liefert reines JSON ohne __TypeId__ fuer den Test fremder Produzenten. */
    private org.springframework.amqp.core.Message raw(String json) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType("application/json");
        return new org.springframework.amqp.core.Message(json.getBytes(StandardCharsets.UTF_8), properties);
    }

    /** Liest die wirklichen Broker-Eigenschaften statt nur die Java-Konfiguration zu vergleichen. */
    private JsonNode queueInfo(String name) throws Exception {
        return request("GET", "/api/queues/%2F/" + encode(name));
    }

    /** Zaehlt nur Queue-Bindings des dedizierten Anzeige-Exchange. */
    private List<String> liveBindings() throws Exception {
        JsonNode bindings = request("GET", "/api/exchanges/%2F/chat.delivery/bindings/source");
        List<String> names = new ArrayList<>();
        for (JsonNode binding : bindings) {
            if (binding.path("destination_type").asText().equals("queue")) {
                names.add(binding.path("destination").asText());
            }
        }
        return names;
    }

    /** Erwartet erfolgreiche Management-Abfragen; Fehler duerfen nicht als leere Queue gelten. */
    private JsonNode request(String method, String path) throws Exception {
        HttpResponse<String> response = sendRequest(method, path);
        assertThat(response.statusCode()).isEqualTo(200);
        return inspector.readTree(response.body());
    }

    /** Erlaubt die gezielte Pruefung von geloeschten Queues und Verbindungen. */
    private int requestStatus(String method, String path) throws Exception {
        return sendRequest(method, path).statusCode();
    }

    /** Testcontainer-Zugangsdaten gelten nur fuer den kurzlebigen isolierten Broker. */
    private HttpResponse<String> sendRequest(String method, String path) throws Exception {
        String credentials = RABBIT.getAdminUsername() + ":" + RABBIT.getAdminPassword();
        String authorization = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        String address = "http://" + RABBIT.getHost() + ":" + RABBIT.getMappedPort(15672) + path;
        HttpRequest request = HttpRequest.newBuilder(URI.create(address))
                .header("Authorization", "Basic " + authorization)
                .timeout(Duration.ofSeconds(5))
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** Namen koennen Leerzeichen enthalten und werden als einzelner URL-Pfadteil kodiert. */
    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /** Kein Mock-Consumer: beide Kontexte verwenden dieselben Klassen wie der spaetere Compose-Dienst. */
    @TestConfiguration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    @Import({RabbitConfiguration.class, LiveDeliveryConfiguration.class,
            LiveMessageListener.class, LiveMessageBuffer.class})
    static class BrokerTestApplication {
    }
}
