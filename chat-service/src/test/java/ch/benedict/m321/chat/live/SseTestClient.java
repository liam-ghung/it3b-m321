package ch.benedict.m321.chat.live;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Liest echte HTTP-SSE-Antworten fortlaufend, damit Tests nicht auf ein Antwortende warten. */
class SseTestClient implements AutoCloseable {
    private final HttpClient http;
    private final InputStream body;
    private final Thread reader;
    private final ArrayBlockingQueue<Event> events = new ArrayBlockingQueue<>(10000);
    private final CountDownLatch opened = new CountDownLatch(1);
    private volatile IOException readFailure;
    private volatile boolean closed;
    private final HttpResponse<InputStream> response;

    /** Wartet auf echte HTTP-Header und startet dann genau einen Leser fuer diesen Testclient. */
    SseTestClient(String address) throws Exception {
        http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(address))
                .timeout(Duration.ofSeconds(10)).GET().build();
        response = http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        body = response.body();
        reader = new Thread(this::readEvents, "sse-test-reader");
        reader.setDaemon(true);
        reader.start();
    }

    /** Prueft, dass die Verbindung bereits einen Kommentar und nicht bloss Header geliefert hat. */
    boolean awaitOpened() throws InterruptedException {
        return opened.await(5, TimeUnit.SECONDS);
    }

    /** Gibt ein Ereignis innerhalb der Testfrist zurueck; null bedeutet kein Ereignis. */
    Event next(Duration timeout) throws Exception {
        Event event = events.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (readFailure != null) {
            throw readFailure;
        }
        return event;
    }

    /** Macht die tatsaechlichen Response-Header fuer Formatpruefungen zugaenglich. */
    HttpResponse<InputStream> response() {
        return response;
    }

    /** Kommentare sind keine Nachrichten; id und JSON-data gehoeren bis zur Leerzeile zusammen. */
    private void readEvents() {
        try (BufferedReader input = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
            String id = null;
            String data = null;
            String line;
            while ((line = input.readLine()) != null) {
                if (line.startsWith(":")) {
                    opened.countDown();
                } else if (line.startsWith("id: ")) {
                    id = line.substring(4);
                } else if (line.startsWith("data: ")) {
                    if (data != null) {
                        throw new IOException("Test erwartet genau eine JSON-data-Zeile je Ereignis");
                    }
                    data = line.substring(6);
                } else if (line.isEmpty()) {
                    if (data != null && !events.offer(new Event(id, data))) {
                        throw new IOException("Testclient-Puffer voll");
                    }
                    id = null;
                    data = null;
                }
            }
        } catch (IOException exception) {
            if (!closed) {
                readFailure = exception;
            }
        }
    }

    /** Abbruch des Bodys beendet die Verbindung und gibt den Leser wieder frei. */
    @Override
    public void close() throws Exception {
        closed = true;
        body.close();
        http.shutdownNow();
        reader.join(2000);
    }

    /** Ein vollstaendiges SSE-Datenereignis, noch ohne Interpretation des Nachrichten-JSON. */
    record Event(String id, String data) {
    }
}
