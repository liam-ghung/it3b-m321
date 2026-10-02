package ch.benedict.m321.batch;

import java.util.HashMap;
import java.util.Map;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Legt den Consumer und dessen dauerhafte Fehlerablage explizit fest. */
@Configuration
public class BatchConfiguration {

    /** Auch ein allein gestarteter Writer kann seine Eingangsqueue anlegen. */
    @Bean
    public Queue persistQueue() {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-dead-letter-exchange", "");
        arguments.put("x-dead-letter-routing-key", "chat.dlq");
        return new Queue("chat.persist", true, false, false, arguments);
    }

    /** Endgueltig ungueltige Nachrichten bleiben fuer die Fehlersuche erhalten. */
    @Bean
    public Queue deadLetterQueue() {
        return new Queue("chat.dlq", true);
    }

    /** Eine Instanz bekommt einen Kanal; weitere Instanzen konkurrieren an derselben Queue. */
    @Bean
    public SimpleMessageListenerContainer batchContainer(ConnectionFactory connectionFactory,
            BatchListener listener, @Value("${batch.size}") int size,
            @Value("${batch.timeout-ms}") long timeout) {
        if (size < 1 || timeout < 1) {
            throw new IllegalArgumentException("Batch-Groesse und Zeitlimit muessen positiv sein");
        }
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames("chat.persist");
        container.setConsumerBatchEnabled(true);
        container.setBatchSize(size);
        container.setPrefetchCount(size);
        container.setReceiveTimeout(timeout);
        container.setBatchReceiveTimeout(timeout);
        container.setConcurrentConsumers(1);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener(listener);
        container.setShutdownTimeout(10000);
        return container;
    }
}
