package ch.benedict.m321.batch;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Schreibt einen Stapel atomar; die UUID macht erneute Zustellung harmlos. */
@Repository
public class MessageRepository {
    private static final String INSERT = "INSERT INTO message "
            + "(id, room_id, sender_id, sender_name, content, sent_at) VALUES (?, ?, ?, ?, ?, ?) "
            + "ON CONFLICT (id) DO NOTHING";
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transaction;

    /** Der sichtbare Transaktionsrahmen macht die Reihenfolge vor dem ACK nachvollziehbar. */
    public MessageRepository(JdbcTemplate jdbcTemplate, PlatformTransactionManager manager) {
        this.jdbcTemplate = jdbcTemplate;
        this.transaction = new TransactionTemplate(manager);
    }

    /** Kehrt erst nach COMMIT zurueck; bei Fehler wird der gesamte Stapel zurueckgerollt. */
    public void saveBatch(List<StoredMessage> messages) {
        List<Object[]> parameters = new ArrayList<>();
        for (StoredMessage message : messages) {
            Timestamp sentAt = Timestamp.from(message.sentAt());
            Object[] row = {message.id(), message.roomId(), message.senderId(),
                    message.senderName(), message.content(), sentAt};
            parameters.add(row);
        }
        // Der Callback laeuft innerhalb EINER Transaktion, nicht eine Transaktion pro Zeile.
        transaction.executeWithoutResult(status -> jdbcTemplate.batchUpdate(INSERT, parameters));
    }
}
