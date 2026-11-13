package ch.benedict.m321.chat.live;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import jakarta.annotation.PreDestroy;
import ch.benedict.m321.chat.message.Message;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Schuetzt Empfangsfolge und Raumverteilung gemeinsam, ohne unter der Sperre ins Netzwerk zu schreiben. */
@Component
public class LiveMessageBuffer {
    private static final int MAX_MESSAGES = 10000;
    private static final long MAX_AGE_NANOS = TimeUnit.SECONDS.toNanos(120);
    private final Map<UUID, ReceivedMessage> messages = new LinkedHashMap<>();
    private final Map<UUID, List<LiveStreamClient>> clientsByRoom = new LinkedHashMap<>();
    private int clientCount;
    private final LongSupplier clock;

    /** Im Betrieb misst die monotone Uhr die Aufbewahrungsdauer unabhaengig von Uhrkorrekturen. */
    public LiveMessageBuffer() {
        this(System::nanoTime);
    }

    /** Eine kontrollierte Uhr erlaubt Ablaufpruefungen ohne minutenlange Wartezeiten im Test. */
    LiveMessageBuffer(LongSupplier clock) {
        this.clock = clock;
    }

    /** Ohne Cursor beginnt die Verbindung ausschliesslich mit neuen Live-Nachrichten. */
    public LiveStreamClient register(UUID roomId) {
        return register(roomId, null);
    }

    /** Nachlieferung und Registrierung teilen die Empfangssperre: neue Nachrichten landen danach hinten. */
    public synchronized LiveStreamClient register(UUID roomId, UUID lastEventId) {
        removeExpired(clock.getAsLong());
        if (lastEventId != null) {
            ReceivedMessage anchor = messages.get(lastEventId);
            if (anchor == null || !anchor.message().roomId().equals(roomId)) {
                throw new ResponseStatusException(HttpStatus.CONFLICT,
                        "Letzte Nachrichten-ID nicht mehr fuer diesen Raum verfuegbar; Verlauf neu laden");
            }
        }
        if (clientCount >= 100) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Hoechstens 100 gleichzeitige Live-Verbindungen pro Instanz");
        }
        LiveStreamClient client = new LiveStreamClient(roomId, 10000);
        if (lastEventId != null) {
            boolean afterAnchor = false;
            for (ReceivedMessage received : messages.values()) {
                Message message = received.message();
                if (afterAnchor && message.roomId().equals(roomId)) {
                    // Mit Anker im maximal 10000er-Puffer passen hoechstens 9999 Folgeereignisse hinein.
                    client.offer(message);
                }
                if (message.id().equals(lastEventId)) {
                    afterAnchor = true;
                }
            }
        }
        List<LiveStreamClient> clients = clientsByRoom.get(roomId);
        if (clients == null) {
            clients = new ArrayList<>();
            clientsByRoom.put(roomId, clients);
        }
        clients.add(client);
        clientCount++;
        return client;
    }

    /** Wiederholte Abschlussmeldungen sind harmlos und entfernen eine Verbindung hoechstens einmal. */
    public synchronized void unregister(LiveStreamClient client) {
        client.close();
        List<LiveStreamClient> clients = clientsByRoom.get(client.roomId());
        if (clients != null && clients.remove(client)) {
            clientCount--;
            if (clients.isEmpty()) {
                clientsByRoom.remove(client.roomId());
            }
        }
    }

    /** Erlaubt Tests und Diagnose, auf die tatsaechliche Ressourcenfreigabe zu warten. */
    public synchronized int connectionCount() {
        return clientCount;
    }

    /** Uebernimmt eine neue ID in Empfangsreihenfolge, bevor der Broker ein ACK bekommt. */
    public synchronized void accept(Message message) {
        long now = clock.getAsLong();
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
        distribute(message);
    }

    /** Legt nur in passende Client-Puffer; langsame Clients blockieren keine anderen Empfaenger. */
    private void distribute(Message message) {
        List<LiveStreamClient> clients = clientsByRoom.get(message.roomId());
        if (clients == null) {
            return;
        }
        Iterator<LiveStreamClient> iterator = clients.iterator();
        while (iterator.hasNext()) {
            LiveStreamClient client = iterator.next();
            if (!client.offer(message)) {
                iterator.remove();
                clientCount--;
            }
        }
        if (clients.isEmpty()) {
            clientsByRoom.remove(message.roomId());
        }
    }

    /** Gibt eine unabhaengige Momentaufnahme zur Pruefung des empfangenen Stapels zurueck. */
    public synchronized List<Message> snapshot() {
        removeExpired(clock.getAsLong());
        List<Message> result = new ArrayList<>();
        for (ReceivedMessage received : messages.values()) {
            result.add(received.message());
        }
        return result;
    }

    /** Raeumt auch ohne neue Nachrichten auf, damit alte Inhalte nicht unbegrenzt liegen bleiben. */
    @Scheduled(fixedDelay = 10000)
    public synchronized void expire() {
        removeExpired(clock.getAsLong());
    }

    /** Beendet bei Verbindungsverlust oder vor dem Server-Shutdown auch noch offene Streams. */
    @PreDestroy
    @EventListener(ContextClosedEvent.class)
    public synchronized void clear() {
        messages.clear();
        for (List<LiveStreamClient> clients : clientsByRoom.values()) {
            for (LiveStreamClient client : clients) {
                client.close();
            }
        }
        clientsByRoom.clear();
        clientCount = 0;
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
