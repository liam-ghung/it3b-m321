package ch.benedict.m321.chat.message;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import ch.benedict.m321.chat.rabbit.RabbitConfiguration;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;

import static org.assertj.core.api.Assertions.assertThat;

/** Sichert den bestehenden JSON-Vertrag vor der Erweiterung um Live-Zustellung. */
@JsonTest
class MessageContractTest {
    @Autowired
    private ObjectMapper objectMapper;

    /** API-JSON und Broker-Body muessen dieselben sechs Felder und Werte enthalten. */
    @Test
    void apiAndBrokerPreserveCanonicalMessageIncludingEscapedText() throws Exception {
        UUID id = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
        UUID roomId = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000001");
        Instant sentAt = Instant.parse("2026-11-13T07:00:00.123456Z");
        String content = "Hallo \"Liam\"\nZweite Zeile: Grüsse";
        Message message = new Message(id, roomId, "student-1", "Liam", content, sentAt);
        String apiJson = objectMapper.writeValueAsString(message);
        ObjectNode expected = objectMapper.createObjectNode();
        expected.put("id", id.toString());
        expected.put("roomId", roomId.toString());
        expected.put("senderId", "student-1");
        expected.put("senderName", "Liam");
        expected.put("content", content);
        expected.put("sentAt", sentAt.toString());

        RabbitConfiguration configuration = new RabbitConfiguration();
        Jackson2JsonMessageConverter converter = configuration.jsonMessageConverter(objectMapper);
        org.springframework.amqp.core.Message delivery = converter.toMessage(message, new MessageProperties());
        String brokerJson = new String(delivery.getBody(), StandardCharsets.UTF_8);

        assertThat(objectMapper.readTree(apiJson)).isEqualTo(expected);
        assertThat(objectMapper.readTree(brokerJson)).isEqualTo(expected);
        assertThat(delivery.getMessageProperties().getContentType()).isEqualTo("application/json");
        assertThat(brokerJson).doesNotContain("\n", "\r");
        assertThat(objectMapper.readValue(brokerJson, Message.class)).isEqualTo(message);
    }
}
