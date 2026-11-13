package ch.benedict.m321.chat.live;

import ch.benedict.m321.chat.message.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Prueft Cursor-Grenzen und den Uebergang vor dem ersten Netzwerkschreiben deterministisch. */
class LiveReplayBufferTest {
    /** Neue Ereignisse nach Registrierung stehen hinter der Nachlieferung; Dubletten und fremde Raeume fehlen. */
    @Test
    void replayAndNewMessagesAreQueuedOnceBeforeWriterStarts() throws Exception {
        LiveMessageBuffer buffer = new LiveMessageBuffer();
        UUID room = UUID.randomUUID();
        Message anchor = example(room, "Anker");
        Message old = example(room, "Nachlieferung");
        Message current = example(room, "Live");
        buffer.accept(anchor);
        buffer.accept(old);
        buffer.accept(example(UUID.randomUUID(), "Anderer Raum"));
        LiveStreamClient client = buffer.register(room, anchor.id());
        buffer.accept(old);
        buffer.accept(current);
        ByteArrayOutputStream output = new ByteArrayOutputStream() {
            private int events;

            /** Nach zwei echten Datenrahmen wird der Testsender ohne Schlafpause beendet. */
            @Override
            public synchronized void write(byte[] bytes, int offset, int length) {
                super.write(bytes, offset, length);
                String frame = new String(bytes, offset, length, StandardCharsets.UTF_8);
                if (frame.startsWith("id: ")) {
                    events++;
                    if (events == 2) {
                        client.close();
                    }
                }
            }
        };
        client.writeTo(output, new ObjectMapper().findAndRegisterModules());
        String result = output.toString(StandardCharsets.UTF_8);
        assertThat(result).doesNotContain(anchor.id().toString(), "Anderer Raum");
        assertThat(result.split("\ndata: ")).hasSize(3);
        assertThat(result.indexOf(old.id().toString())).isLessThan(result.indexOf(current.id().toString()));
        buffer.clear();
    }

    /** Ablauf gilt exakt ab 120 Sekunden; andere Raeume und invalidierter Zustand liefern ebenfalls 409. */
    @Test
    void rejectsExpiredForeignUnknownAndClearedCursorsWithoutRegistering() {
        AtomicLong now = new AtomicLong();
        LiveMessageBuffer buffer = new LiveMessageBuffer(now::get);
        UUID room = UUID.randomUUID();
        Message anchor = example(room, "Anker");
        buffer.accept(anchor);
        reject(buffer, room, UUID.randomUUID());
        reject(buffer, UUID.randomUUID(), anchor.id());
        now.set(TimeUnit.SECONDS.toNanos(120) - 1);
        buffer.unregister(buffer.register(room, anchor.id()));
        now.incrementAndGet();
        reject(buffer, room, anchor.id());
        Message fresh = example(room, "Frisch");
        buffer.accept(fresh);
        buffer.clear();
        reject(buffer, room, fresh.id());
        assertThat(buffer.connectionCount()).isZero();
    }

    /** Verdrängte Anker duerfen nicht stillschweigend einen unvollstaendigen Verlauf liefern. */
    @Test
    void capacityEvictionRejectsAnchor() {
        LiveMessageBuffer buffer = new LiveMessageBuffer();
        UUID room = UUID.randomUUID();
        Message anchor = example(room, "Anker");
        buffer.accept(anchor);
        for (int index = 0; index < 10000; index++) {
            buffer.accept(example(room, "Nachricht " + index));
        }
        reject(buffer, room, anchor.id());
        assertThat(buffer.connectionCount()).isZero();
    }

    /** Vergleicht den vereinbarten Status statt nur irgendeine Ausnahme zu erwarten. */
    private void reject(LiveMessageBuffer buffer, UUID room, UUID cursor) {
        assertThatThrownBy(() -> buffer.register(room, cursor))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode().value()).isEqualTo(409));
    }

    /** Jede Testnachricht besitzt eine neue ID bei bewusst gleichem Zeitstempel. */
    private Message example(UUID room, String text) {
        return new Message(UUID.randomUUID(), room, "student", "Liam", text, Instant.EPOCH);
    }
}
