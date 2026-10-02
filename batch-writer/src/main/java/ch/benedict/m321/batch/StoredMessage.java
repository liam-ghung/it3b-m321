package ch.benedict.m321.batch;

import java.time.Instant;
import java.util.UUID;

/** Eigene Sicht auf den JSON-Vertrag; der Writer braucht keine Producer-Klassen. */
public record StoredMessage(UUID id, UUID roomId, String senderId,
                            String senderName, String content, Instant sentAt) {

    /** Fehlerhafte Daten werden vor dem SQL erkannt, damit gesunde Nachbarn weiterkommen. */
    public void validate() {
        if (id == null || roomId == null || sentAt == null) {
            throw new IllegalArgumentException("ID, Raum oder Sendezeit fehlt");
        }
        if (senderId == null || senderId.isBlank() || senderId.length() > 255) {
            throw new IllegalArgumentException("Absender-ID ungueltig");
        }
        if (senderName == null || senderName.isBlank() || senderName.length() > 255) {
            throw new IllegalArgumentException("Absendername ungueltig");
        }
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("Inhalt fehlt");
        }
    }
}
