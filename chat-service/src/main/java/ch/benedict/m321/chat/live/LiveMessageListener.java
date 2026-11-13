package ch.benedict.m321.chat.live;

import java.io.IOException;
import ch.benedict.m321.chat.message.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.stereotype.Component;

/** Liest eine reine JSON-Lieferung und bestaetigt erst nach der lokalen Uebernahme. */
@Component
public class LiveMessageListener implements ChannelAwareMessageListener {
    private static final Logger log = LoggerFactory.getLogger(LiveMessageListener.class);
    private final ObjectMapper objectMapper;
    private final LiveMessageBuffer buffer;

    /** Der gemeinsame JSON-Vertrag braucht keinen Java-Typheader des Absenders. */
    public LiveMessageListener(ObjectMapper objectMapper, LiveMessageBuffer buffer) {
        this.objectMapper = objectMapper;
        this.buffer = buffer;
    }

    /** Kaputtes JSON wird verworfen; Kanalfehler werden an den Container zur Wiederherstellung gegeben. */
    @Override
    public void onMessage(org.springframework.amqp.core.Message delivery, Channel channel) throws IOException {
        long tag = delivery.getMessageProperties().getDeliveryTag();
        Message message;
        try {
            message = objectMapper.readValue(delivery.getBody(), Message.class);
            validate(message);
        } catch (IOException | IllegalArgumentException exception) {
            log.warn("Ungueltige Live-Nachricht wird verworfen: {}", exception.getMessage());
            channel.basicReject(tag, false);
            return;
        }
        buffer.accept(message);
        // Dieses ACK bestaetigt den lokalen Empfang, nicht das Lesen durch einen Benutzer.
        channel.basicAck(tag, false);
    }

    /** Direkte Broker-Publishes muessen denselben Pflichtvertrag wie HTTP-Nachrichten erfuellen. */
    private void validate(Message message) {
        if (message == null || message.id() == null || message.roomId() == null || message.sentAt() == null) {
            throw new IllegalArgumentException("Nachricht, ID, Raum oder Zeitpunkt fehlt");
        }
        if (message.senderId() == null || message.senderId().isBlank() || message.senderId().length() > 255) {
            throw new IllegalArgumentException("Absender-ID ungueltig");
        }
        if (message.senderName() == null || message.senderName().isBlank() || message.senderName().length() > 255) {
            throw new IllegalArgumentException("Absendername ungueltig");
        }
        if (message.content() == null || message.content().isBlank()) {
            throw new IllegalArgumentException("Nachrichteninhalt fehlt");
        }
    }
}
