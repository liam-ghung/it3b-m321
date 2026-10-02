package ch.benedict.m321.chat.message;

import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonAlias;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Was der Client beim Senden mitschickt. Bewusst NICHT dasselbe wie Message:
 * id und sentAt vergibt der Server, nicht der Client. Wuerde der Client sie
 * mitschicken duerfen, koennte er sich eine fremde Uhrzeit oder eine fremde ID aussuchen.
 */
public record NewMessage(

        @Schema(description = "In welchen Raum die Nachricht gehoert",
                example = "11111111-1111-1111-1111-111111111111")
        UUID roomId,

        @Schema(description = "PLATZHALTER bis Keycloak da ist. Danach kommt der Absender "
                            + "aus dem Token und dieses Feld faellt ersatzlos weg.",
                example = "lernende1")
        @JsonAlias("sender") String senderId,

        @Schema(description = "Anzeigename des Absenders") String senderName,

        @Schema(description = "Der Nachrichtentext", example = "Hallo zusammen")
        @JsonAlias("text") String content) {

    /** Ohne separaten Anzeigenamen dient wie beim Bootstrap die ID als Name. */
    public NewMessage {
        if (senderName == null) {
            senderName = senderId;
        }
    }

    /** Erhaelt bestehende Java-Aufrufer aus dem Bootstrap. */
    public NewMessage(UUID roomId, String sender, String text) {
        this(roomId, sender, sender, text);
    }

    /** Alte Aufrufer erhalten weiterhin die Absender-ID. */
    public String sender() {
        return senderId;
    }

    /** Alte Aufrufer erhalten weiterhin den Inhalt. */
    public String text() {
        return content;
    }
}
