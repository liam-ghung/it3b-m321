package ch.benedict.m321.chat.live;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import ch.benedict.m321.chat.rabbit.RabbitConfiguration;
import com.rabbitmq.client.ShutdownSignalException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Ergaenzt den Anzeigeweg, ohne die dauerhafte Speicherung umzubinden. */
@Configuration
@EnableScheduling
public class LiveDeliveryConfiguration {
    public static final String EXCHANGE_NAME = "chat.delivery";

    /** Ein eigener Anzeige-Exchange erlaubt eine eindeutige Zaehlung der Live-Queues. */
    @Bean
    public FanoutExchange deliveryExchange() {
        return new FanoutExchange(EXCHANGE_NAME, true, false);
    }

    /** RabbitMQ verteilt einen einzigen Publish gleichzeitig zum Speicher- und Anzeigeweg. */
    @Bean
    public Binding deliveryExchangeBinding() {
        return new Binding(EXCHANGE_NAME, Binding.DestinationType.EXCHANGE,
                RabbitConfiguration.EXCHANGE_NAME, "", null);
    }

    /** 999 wartende und hoechstens eine unbestaetigte Lieferung begrenzen die Gesamtzahl auf 1000. */
    @Bean
    public Queue liveQueue() {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-queue-type", "classic");
        arguments.put("x-max-length", 999);
        arguments.put("x-overflow", "drop-head");
        arguments.put("x-message-ttl", 30000);
        String name = "chat.live." + UUID.randomUUID();
        return new Queue(name, false, true, true, arguments);
    }

    /** Jede Instanz bindet nur ihre eigene Queue an denselben Anzeige-Exchange. */
    @Bean
    public Binding liveBinding(@Qualifier("liveQueue") Queue queue) {
        return new Binding(queue.getName(), Binding.DestinationType.QUEUE, EXCHANGE_NAME, "", null);
    }

    /** Ein Consumer erhaelt die Reihenfolge; Spring deklariert die Queue nach Verbindungsverlust neu. */
    @Bean
    public SimpleMessageListenerContainer liveContainer(ConnectionFactory connectionFactory,
            LiveMessageListener listener, LiveMessageBuffer buffer,
            @Qualifier("liveQueue") Queue queue,
            @Value("${spring.rabbitmq.listener.simple.auto-startup:true}") boolean autoStartup) {
        connectionFactory.addConnectionListener(new ConnectionListener() {
            /** Eine neue Verbindung beginnt nach der bereits ausgefuehrten Invalidierung. */
            @Override
            public void onCreate(Connection connection) {
                // Kein Netzwerkzugriff: RabbitAdmin uebernimmt die Deklarationen.
            }

            /** Beim geordneten Schliessen darf der alte Empfangszustand nicht weiterleben. */
            @Override
            public void onClose(Connection connection) {
                buffer.clear();
            }

            /** Auch ein ungeplanter Broker-Abbruch kann eine Luecke verursacht haben. */
            @Override
            public void onShutDown(ShutdownSignalException signal) {
                buffer.clear();
            }
        });
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(queue);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(1);
        container.setPrefetchCount(1);
        container.setBatchSize(1);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener(listener);
        container.setAutoStartup(autoStartup);
        container.setRecoveryInterval(1000);
        container.setShutdownTimeout(5000);
        return container;
    }
}
