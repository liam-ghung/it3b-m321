package ch.benedict.m321.chat.message;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** Prueft die Trennung von Senden und Datenbankzugriff ohne externe Dienste. */
class MessageServiceTest {

    /** Eine gesendete Nachricht erhaelt Serverwerte und geht nur an den Broker. */
    @Test
    void sendPublishesWithServerIdAndTimeWithoutDatabaseAccess() {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class);
        MessageRepository repository = mock(MessageRepository.class);
        MessageService service = new MessageService(rabbitTemplate, repository);
        UUID roomId = UUID.randomUUID();
        NewMessage incoming = new NewMessage(roomId, "lernende1", "Hallo");
        Instant before = Instant.now();

        Message sent = service.sendMessage(incoming);

        assertThat(sent.id()).isNotNull();
        assertThat(sent.sentAt()).isBetween(before, Instant.now());
        assertThat(sent.roomId()).isEqualTo(roomId);
        assertThat(sent.sender()).isEqualTo("lernende1");
        assertThat(sent.text()).isEqualTo("Hallo");
        verify(rabbitTemplate).convertAndSend("chat.messages", "", sent);
        verifyNoInteractions(repository);
    }
}
