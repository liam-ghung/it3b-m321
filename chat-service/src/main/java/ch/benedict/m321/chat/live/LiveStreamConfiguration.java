package ch.benedict.m321.chat.live;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Begrenzt HTTP-Sender und bereinigt auch abgebrochene asynchrone Anfragen. */
@Configuration
public class LiveStreamConfiguration implements WebMvcConfigurer {
    private final ThreadPoolTaskExecutor executor;
    private final LiveMessageBuffer buffer;

    /** Der feste Pool trennt Socket-Schreiben vom RabbitMQ-Consumer und vom Scheduler. */
    public LiveStreamConfiguration(@Qualifier("liveStreamExecutor") ThreadPoolTaskExecutor executor,
            LiveMessageBuffer buffer) {
        this.executor = executor;
        this.buffer = buffer;
    }

    /** Auch die kurze Uebergangs-Warteschlange beim Beenden alter Worker bleibt begrenzt. */
    @Bean
    public static ThreadPoolTaskExecutor liveStreamExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(100);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("live-sse-");
        executor.setWaitForTasksToCompleteOnShutdown(false);
        executor.setAwaitTerminationSeconds(6);
        return executor;
    }

    /** Eine Verbindung endet spaetestens nach 30 Minuten; im Leerlauf laufen weiterhin Heartbeats. */
    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setTaskExecutor(executor);
        configurer.setDefaultTimeout(1800000);
    }

    /** Erfasst auch Timeout oder Executor-Ablehnung, bevor der Schreib-Callback beginnen konnte. */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new HandlerInterceptor() {
            /** Nach der letzten HTTP-Dispatch-Phase wird eine verbliebene Registrierung entfernt. */
            @Override
            public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                    Object handler, Exception exception) {
                Object attribute = request.getAttribute(LiveStreamController.CLIENT_ATTRIBUTE);
                if (attribute instanceof LiveStreamClient client) {
                    buffer.unregister(client);
                }
            }
        }).addPathPatterns("/stream");
    }
}
