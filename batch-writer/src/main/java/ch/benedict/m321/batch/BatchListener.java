package ch.benedict.m321.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareBatchMessageListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

/** Verknuepft Datenbank-COMMIT mit Broker-ACK; ohne COMMIT wird nichts bestaetigt. */
@Component
public class BatchListener implements ChannelAwareBatchMessageListener {
    private static final Logger log = LoggerFactory.getLogger(BatchListener.class);
    private final ObjectMapper objectMapper;
    private final MessageRepository repository;
    private final long retryDelay;

    /** JSON wird direkt gelesen, ohne Java-Typheader eines fremden Dienstes zu vertrauen. */
    public BatchListener(ObjectMapper objectMapper, MessageRepository repository,
            @Value("${batch.retry-delay-ms}") long retryDelay) {
        this.objectMapper = objectMapper;
        this.repository = repository;
        if (retryDelay < 1) {
            throw new IllegalArgumentException("Wiederholungspause muss positiv sein");
        }
        this.retryDelay = retryDelay;
    }

    /** Jede Broker-Lieferung behaelt ihren Kanal und Tag bis zur erfolgreichen Verarbeitung. */
    @Override
    public void onMessageBatch(List<Message> deliveries, Channel channel) {
        try {
            List<PendingMessage> valid = decode(deliveries, channel);
            if (!valid.isEmpty()) {
                persist(valid, channel);
            }
        } catch (IOException exception) {
            // Bei Kanalverlust bleiben noch unbestaetigte Lieferungen im Broker erhalten.
            throw new AmqpIOException(exception);
        }
    }

    /** Nur die kaputte Lieferung geht in die DLQ; alle gesunden bleiben im Stapel. */
    private List<PendingMessage> decode(List<Message> deliveries, Channel channel) throws IOException {
        List<PendingMessage> valid = new ArrayList<>();
        for (Message delivery : deliveries) {
            long tag = delivery.getMessageProperties().getDeliveryTag();
            try {
                StoredMessage message = objectMapper.readValue(delivery.getBody(), StoredMessage.class);
                if (message == null) {
                    throw new IllegalArgumentException("JSON null ist keine Nachricht");
                }
                message.validate();
                valid.add(new PendingMessage(message, tag));
            } catch (IOException | IllegalArgumentException exception) {
                log.warn("Ungueltige Nachricht nach chat.dlq: {}", exception.getMessage());
                channel.basicReject(tag, false);
            }
        }
        return valid;
    }

    /** Bestaetigt erst nach erfolgreicher Rueckkehr aus der Datenbanktransaktion. */
    private void persist(List<PendingMessage> pending, Channel channel) throws IOException {
        List<StoredMessage> messages = new ArrayList<>();
        for (PendingMessage delivery : pending) {
            messages.add(delivery.message());
        }
        try {
            repository.saveBatch(messages);
        } catch (DataIntegrityViolationException exception) {
            isolateInvalidRows(pending, channel);
            return;
        } catch (RuntimeException exception) {
            retryLater(pending, channel, exception);
            return;
        }
        for (PendingMessage delivery : pending) {
            channel.basicAck(delivery.tag(), false);
        }
        log.info("Stapel mit {} Nachrichten committed und bestaetigt", pending.size());
    }

    /** Nach einem Datenfehler wird einmal pro Zeile versucht, damit gesunde Nachbarn nicht scheitern. */
    private void isolateInvalidRows(List<PendingMessage> pending, Channel channel) throws IOException {
        for (PendingMessage delivery : pending) {
            List<StoredMessage> single = List.of(delivery.message());
            try {
                repository.saveBatch(single);
            } catch (DataIntegrityViolationException exception) {
                log.warn("Datenfehler bei {}: nach chat.dlq", delivery.message().id());
                channel.basicReject(delivery.tag(), false);
                continue;
            } catch (RuntimeException exception) {
                List<PendingMessage> retry = List.of(delivery);
                retryLater(retry, channel, exception);
                continue;
            }
            channel.basicAck(delivery.tag(), false);
        }
    }

    /** Eine Pause verhindert eine heisse Fehlerschleife; NACK bewahrt die Nachrichten im Broker. */
    private void retryLater(List<PendingMessage> pending, Channel channel, RuntimeException failure)
            throws IOException {
        log.warn("Datenbank-Schreiben fehlgeschlagen, {} Nachrichten werden wiederholt: {}",
                pending.size(), failure.getMessage());
        try {
            Thread.sleep(retryDelay);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        for (PendingMessage delivery : pending) {
            channel.basicNack(delivery.tag(), false, true);
        }
    }

    /** Haelt die Zuordnung zwischen geprueftem Inhalt und genau seiner Broker-Bestaetigung. */
    private record PendingMessage(StoredMessage message, long tag) {
    }
}
