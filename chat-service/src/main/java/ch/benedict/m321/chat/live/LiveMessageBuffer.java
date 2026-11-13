package ch.benedict.m321.chat.live;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import ch.benedict.m321.chat.message.Message;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Begrenzt den lokalen Empfangszustand; SSE und Cursor-Nachlieferung folgen in eigenen Schritten. */
@Component
public class LiveMessageBuffer {
    private static final int MAX_MESSAGES = 10000;
    private static final long MAX_AGE_NANOS = TimeUnit.SECONDS.toNanos(120);
    private final Map<UUID, ReceivedMessage> messages = new LinkedHashMap<>();

    /** Uebernimmt eine neue ID in Empfangsreihenfolge, bevor der Broker ein ACK bekommt. */
    public synchronized void accept(Message message) {
        long now = System.nanoTime();
        removeExpired(now);
        if (messages.containsKey(message.id())) {
            return;
        }
        if (messages.size() >= MAX_MESSAGES) {
            Iterator<UUID> oldest = messages.keySet().iterator();
            oldest.next();
            oldest.remove();
        }
        messages.put(message.id(), new ReceivedMessage(message, now));
    }

    /** Gibt eine unabhaengige Momentaufnahme zur Pruefung des empfangenen Stapels zurueck. */
    public synchronized List<Message> snapshot() {
        removeExpired(System.nanoTime());
        List<Message> result = new ArrayList<>();
        for (ReceivedMessage received : messages.values()) {
            result.add(received.message());
        }
        return result;
    }

    /** Raeumt auch ohne neue Nachrichten auf, damit alte Inhalte nicht unbegrenzt liegen bleiben. */
    @Scheduled(fixedDelay = 10000)
    public synchronized void expire() {
        removeExpired(System.nanoTime());
    }

    /** Verwirft bei Verbindungsverlust einen moeglicherweise lueckenhaften Empfangszustand. */
    public synchronized void clear() {
        messages.clear();
    }

    /** Nutzt monotone Laufzeit statt Systemuhr, damit Uhrkorrekturen die Aufbewahrung nicht verlaengern. */
    private void removeExpired(long now) {
        Iterator<ReceivedMessage> entries = messages.values().iterator();
        while (entries.hasNext()) {
            ReceivedMessage received = entries.next();
            if (now - received.receivedAt() < MAX_AGE_NANOS) {
                break;
            }
            entries.remove();
        }
    }

    /** Haelt die Nachricht zusammen mit ihrem lokalen Empfangszeitpunkt fest. */
    private record ReceivedMessage(Message message, long receivedAt) {
    }
}
