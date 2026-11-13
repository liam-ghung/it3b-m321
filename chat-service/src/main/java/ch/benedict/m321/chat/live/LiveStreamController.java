package ch.benedict.m321.chat.live;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

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
    public ResponseEntity<StreamingResponseBody> stream(@RequestParam UUID roomId, HttpServletRequest request) {
        LiveStreamClient client = buffer.register(roomId);
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
}
