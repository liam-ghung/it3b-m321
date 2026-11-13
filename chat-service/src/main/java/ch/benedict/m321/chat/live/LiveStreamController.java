package ch.benedict.m321.chat.live;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.server.ResponseStatusException;

/** Oeffnet einen raumbezogenen SSE-Stream, dessen Netzwerkzugriffe ein eigener Worker ausfuehrt. */
@RestController
public class LiveStreamController {
    public static final String CLIENT_ATTRIBUTE = LiveStreamController.class.getName() + ".client";
    private final LiveMessageBuffer buffer;
    private final ObjectMapper objectMapper;

    /** Verwendet denselben JSON-Konverter wie der vorhandene Nachrichtenvertrag. */
    public LiveStreamController(LiveMessageBuffer buffer, ObjectMapper objectMapper) {
        this.buffer = buffer;
        this.objectMapper = objectMapper;
    }

    /** Prueft Raum und Kapazitaet vor HTTP 200; nach Abschluss wird der Client sicher entfernt. */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<StreamingResponseBody> stream(@RequestParam UUID roomId,
            @RequestHeader(value = "Last-Event-ID", required = false) String lastEventId,
            HttpServletRequest request) {
        LiveStreamClient client = buffer.register(roomId, parseCursor(lastEventId));
        request.setAttribute(CLIENT_ATTRIBUTE, client);
        StreamingResponseBody body = output -> {
            try {
                client.writeTo(output, objectMapper);
            } finally {
                buffer.unregister(client);
            }
        };
        MediaType contentType = new MediaType("text", "event-stream", StandardCharsets.UTF_8);
        return ResponseEntity.ok().contentType(contentType)
                .header(HttpHeaders.CACHE_CONTROL, "no-cache").body(body);
    }

    /** Nur ein fehlender Header bedeutet keinen Cursor; fehlerhafte oder verkuerzte UUIDs ergeben 400. */
    private UUID parseCursor(String value) {
        if (value == null) {
            return null;
        }
        try {
            UUID id = UUID.fromString(value);
            if (!id.toString().equalsIgnoreCase(value)) {
                throw new IllegalArgumentException("UUID nicht vollstaendig");
            }
            return id;
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Last-Event-ID muss eine UUID sein");
        }
    }
}
