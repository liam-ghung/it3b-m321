package ch.benedict.m321.chat.message;

import java.time.Instant;
import java.util.UUID;

/**
 * Eine Nachricht, so wie sie in der Datenbank steht und wie sie ueber die API
 * herausgeht. Ein "record" ist eine Kurzform fuer eine Klasse, die nur Daten haelt:
 * Java erzeugt Konstruktor, Lesemethoden, equals und toString selbst.
 * Die Felder sind unveraenderlich - einmal gesetzt, bleibt eine Nachricht, wie sie ist.
 */
public record Message(
        UUID id,
        UUID roomId,
        String senderId,
        String senderName,
        String content,
        Instant sentAt) {

    /** Erhaelt Java-Aufrufer aus dem Bootstrap mit nur einem Absendernamen. */
    public Message(UUID id, UUID roomId, String sender, String text, Instant sentAt) {
        this(id, roomId, sender, sender, text, sentAt);
    }

    /** Alte Java-Aufrufer koennen den Anzeigenamen weiterhin lesen. */
    public String sender() {
        return senderName;
    }

    /** Alte Java-Aufrufer koennen den Inhalt weiterhin lesen. */
    public String text() {
        return content;
    }
}
