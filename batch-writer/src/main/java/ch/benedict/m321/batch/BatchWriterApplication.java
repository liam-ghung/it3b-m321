package ch.benedict.m321.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Startet den eigenstaendigen Consumer ohne Webserver. */
@SpringBootApplication
public class BatchWriterApplication {

    /** Spring verbindet Konfiguration, Listener und Datenbankzugriff. */
    public static void main(String[] args) {
        SpringApplication.run(BatchWriterApplication.class, args);
    }
}
