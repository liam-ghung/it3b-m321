package ch.benedict.m321.chat.live;

import java.time.Instant;
import java.util.UUID;
import ch.benedict.m321.chat.message.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Prueft Ueberlauf unabhaengig davon, wie viel ein Betriebssystem im TCP-Puffer zwischenlagert. */
class LiveStreamClientTest {

    /** Die echte 10000er-Grenze entfernt nur den vollen Client und begrenzt zugleich den Wiederholungspuffer. */
    @Test
    void productionCapacityRemovesFullClientAndOtherRoomContinues() {
        LiveMessageBuffer buffer = new LiveMessageBuffer();
        UUID slowRoom = UUID.randomUUID();
        UUID healthyRoom = UUID.randomUUID();
        LiveStreamClient slow = buffer.register(slowRoom);
        LiveStreamClient healthy = buffer.register(healthyRoom);
        for (int index = 0; index < 10000; index++) {
            Message message = new Message(UUID.randomUUID(), slowRoom, "student", "Liam",
                    "Nachricht " + index, Instant.now());
            buffer.accept(message);
        }
        assertThat(slow.isClosed()).isFalse();
        assertThat(buffer.connectionCount()).isEqualTo(2);
        buffer.accept(new Message(UUID.randomUUID(), slowRoom, "student", "Liam", "Ueberlauf", Instant.now()));
        assertThat(slow.isClosed()).isTrue();
        assertThat(buffer.connectionCount()).isEqualTo(1);
        Message next = new Message(UUID.randomUUID(), healthyRoom, "student", "Liam", "Weiter", Instant.now());
        buffer.accept(next);
        assertThat(healthy.isClosed()).isFalse();
        assertThat(buffer.snapshot()).hasSize(10000).contains(next);
        buffer.clear();
    }

    /** Ein kleiner voller Puffer schliesst nur den betroffenen Client und nimmt nichts mehr an. */
    @Test
    void fullClientClosesWithoutBlockingAnotherClient() {
        UUID room = UUID.randomUUID();
        LiveStreamClient slow = new LiveStreamClient(room, 2);
        LiveStreamClient healthy = new LiveStreamClient(room, 2);
        Message message = new Message(UUID.randomUUID(), room, "student", "Liam", "Hallo", Instant.now());
        assertThat(slow.offer(message)).isTrue();
        assertThat(slow.offer(message)).isTrue();
        assertThat(slow.offer(message)).isFalse();
        assertThat(slow.isClosed()).isTrue();
        assertThat(slow.offer(message)).isFalse();
        assertThat(healthy.offer(message)).isTrue();
        assertThat(healthy.isClosed()).isFalse();
        healthy.close();
    }

    /** Der Broker-Verbindungsverlust beendet auch die registrierten SSE-Verbindungen. */
    @Test
    void clearingReceptionClosesAllClientsAndAllowsNewConnections() {
        LiveMessageBuffer buffer = new LiveMessageBuffer();
        LiveStreamClient first = buffer.register(UUID.randomUUID());
        LiveStreamClient second = buffer.register(UUID.randomUUID());
        buffer.clear();
        assertThat(first.isClosed()).isTrue();
        assertThat(second.isClosed()).isTrue();
        assertThat(buffer.connectionCount()).isZero();
        buffer.unregister(first);
        assertThat(buffer.connectionCount()).isZero();
        LiveStreamClient replacement = buffer.register(UUID.randomUUID());
        assertThat(replacement.isClosed()).isFalse();
        assertThat(buffer.connectionCount()).isEqualTo(1);
        buffer.clear();
    }
}
