package ch.benedict.m321.chat.live;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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

    /** Registriert nur neue Live-Ereignisse; Cursor-Nachlieferung wird in Schritt 6 ergaenzt. */
    public synchronized LiveStreamClient register(UUID roomId) {
        if (clientCount >= 100) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Hoechstens 100 gleichzeitige Live-Verbindungen pro Instanz");
        }
        LiveStreamClient client = new LiveStreamClient(roomId, 10000);
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
