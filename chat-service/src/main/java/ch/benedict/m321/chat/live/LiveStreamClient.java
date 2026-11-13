package ch.benedict.m321.chat.live;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import ch.benedict.m321.chat.message.Message;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Ein Client hat einen begrenzten Posteingang und genau einen schreibenden HTTP-Worker. */
public class LiveStreamClient {
    private static final Message CLOSED = new Message(null, null, null, null, null, null);
    private final UUID roomId;
    private final ArrayBlockingQueue<Message> pending;
    private volatile boolean closed;

    /** Der Puffer darf kleiner, aber nie groesser als die spezifizierte Obergrenze sein. */
    public LiveStreamClient(UUID roomId, int capacity) {
        if (roomId == null || capacity < 1 || capacity > 10000) {
            throw new IllegalArgumentException("Raum oder Client-Puffergroesse ungueltig");
        }
        this.roomId = roomId;
        this.pending = new ArrayBlockingQueue<>(capacity);
    }

    /** Erlaubt der zentralen Verwaltung, die Verbindung beim Aufraeumen ihrem Raum zuzuordnen. */
    public UUID roomId() {
        return roomId;
    }

    /** Blockiert den Broker niemals; ein voller Puffer beendet nur diese Verbindung. */
    public synchronized boolean offer(Message message) {
        if (closed) {
            return false;
        }
        if (!pending.offer(message)) {
            close();
            return false;
        }
        return true;
    }

    /** Schreibt erst nach Beginn der asynchronen HTTP-Antwort und niemals unter der Raumsperre. */
    public void writeTo(OutputStream output, ObjectMapper objectMapper) throws IOException {
        try {
            if (closed) {
                return;
            }
            writeFrame(output, ": verbunden\n\n");
            long lastHeartbeat = System.nanoTime();
            while (!closed) {
                Message message = pending.poll(5, TimeUnit.SECONDS);
                if (closed) {
                    break;
                }
                if (message != null) {
                    String json = objectMapper.writeValueAsString(message);
                    String frame = "id: " + message.id() + "\ndata: " + json + "\n\n";
                    writeFrame(output, frame);
                }
                long now = System.nanoTime();
                if (message == null || now - lastHeartbeat >= TimeUnit.SECONDS.toNanos(5)) {
                    // Auch bei laufenden Daten sendet derselbe Worker regelmaessig einen Kommentar.
                    writeFrame(output, ": heartbeat\n\n");
                    lastHeartbeat = System.nanoTime();
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    /** Flush liefert jedes vollstaendige Ereignis sofort; Tomcat begrenzt blockierte Socket-Schreibzugriffe. */
    private void writeFrame(OutputStream output, String frame) throws IOException {
        byte[] bytes = frame.getBytes(StandardCharsets.UTF_8);
        output.write(bytes);
        output.flush();
    }

    /** Fuehrt kein Netzwerk-I/O aus und weckt einen wartenden Sender ohne weiteren Thread. */
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        pending.clear();
        pending.offer(CLOSED);
    }

    /** Macht den Verbindungszustand fuer Verwaltung und gezielte Tests sichtbar. */
    public boolean isClosed() {
        return closed;
    }
}
