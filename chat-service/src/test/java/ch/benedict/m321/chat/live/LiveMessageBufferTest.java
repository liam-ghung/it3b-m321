package ch.benedict.m321.chat.live;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import ch.benedict.m321.chat.message.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Prueft die lokale Speichergrenze unabhaengig von Broker-Timing und Netzwerklatenz. */
class LiveMessageBufferTest {

    /** Die aelteste Nachricht weicht bei voller Kapazitaet; eine Dublette verdraengt keinen Nachbarn. */
    @Test
    void capacityEvictsOldestButDuplicateDoesNotChangeOrder() {
        LiveMessageBuffer buffer = new LiveMessageBuffer();
        UUID roomId = UUID.randomUUID();
        Message first = null;
        Message last = null;
        for (int index = 0; index <= 10000; index++) {
            Message message = new Message(UUID.randomUUID(), roomId, "student", "Liam",
                    "Nachricht " + index, Instant.now());
            if (index == 0) {
                first = message;
            }
            last = message;
            buffer.accept(message);
        }
        List<Message> beforeDuplicate = buffer.snapshot();
        assertThat(beforeDuplicate).hasSize(10000);
        assertThat(beforeDuplicate).doesNotContain(first);
        assertThat(beforeDuplicate.get(0).content()).isEqualTo("Nachricht 1");
        buffer.accept(last);
        assertThat(buffer.snapshot()).containsExactlyElementsOf(beforeDuplicate);
    }

    /** Fremde Listenveraenderungen und Verbindungsabbruch duerfen keinen alten Zustand zuruecklassen. */
    @Test
    void snapshotIsIndependentAndClearInvalidatesReception() {
        LiveMessageBuffer buffer = new LiveMessageBuffer();
        Message message = new Message(UUID.randomUUID(), UUID.randomUUID(), "student", "Liam",
                "Hallo", Instant.now());
        buffer.accept(message);
        List<Message> snapshot = buffer.snapshot();
        snapshot.clear();
        assertThat(buffer.snapshot()).containsExactly(message);
        buffer.clear();
        assertThat(buffer.snapshot()).isEmpty();
    }
}
