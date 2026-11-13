# Schritt 2: Eigener Live-Empfang je Instanz

Datum: 13.11.2026. Grundlage: Spezifikation und unveränderter Schritt 2 des
vorab gepushten Plans. Noch kein SSE-Endpunkt und keine Last-Event-ID-Nachlieferung.

## Umgesetzt

```text
Ein POST publiziert einmal an chat.messages
  +-> chat.persist -> Batch-Writer (bestehend, unverändert)
  +-> chat.delivery (Exchange-Binding)
        +-> chat.live.<UUID-A> -> Consumer A -> lokaler Empfangspuffer A
        +-> chat.live.<UUID-B> -> Consumer B -> lokaler Empfangspuffer B
```

LiveDeliveryConfiguration stellt Exchange, Exchange-Binding, eine exklusive
nicht-dauerhafte Auto-delete-Queue und einen sequenziellen Consumer je Instanz
bereit. Die Queue hält höchstens 999 Ready-Nachrichten; Prefetch 1 erlaubt eine
weitere unbestätigte Lieferung. TTL: 30000 ms, Überlauf: drop-head. Diese festen
Grenzen können nicht versehentlich durch Umgebungswerte erhöht werden. Es werden
in diesem Schritt keine neuen Umgebungsvariablen eingeführt.

LiveMessageListener liest JSON ohne erforderlichen Java-Typheader, validiert den
Vertrag und bestätigt erst nach Übernahme durch LiveMessageBuffer. Ungültige
Lieferungen werden ohne Requeue verworfen. Der bestehende Speicherweg wird dadurch
nicht verändert. Ein Live-ACK verspricht keine Anzeige auf einem Endgerät.

LiveMessageBuffer ist die begrenzte lokale Übergabestelle, damit der neue Consumer
Nachrichten nicht bloß bestätigt und vergisst. Er hält höchstens 10000 Nachrichten
für höchstens 120 s im nutzbaren Empfangsfenster und dedupliziert IDs. Abgelaufene
Einträge sind bei Lesen/Schreiben nicht sichtbar; zusätzlich läuft alle 10 s eine
Bereinigung. Monotone Laufzeit verhindert Probleme durch Systemuhrkorrekturen.
Nach Verbindungsabbruch wird dieser Zustand invalidiert. Client-Registrierung,
SSE-Sender und atomare Cursor-Nachlieferung werden erst in Schritten 3 und 6 ergänzt.

Spring RabbitAdmin deklariert die Beans nach Neuverbindung erneut. Der Queue-Name
bleibt innerhalb eines Anwendungskontextes gleich, neue Instanzen erzeugen neue UUIDs.
Fehlende Queue/Verbindung wird vom Listener-Container erneut versucht. Beim
geordneten Kontextende werden Consumer und Verbindung geschlossen.

## Prüfungen und Grenzen des Nachweises

LiveDeliveryIntegrationTest startet einen echten RabbitMQ-Testcontainer und zwei
unabhängige Spring-Kontexte mit den Produktionsklassen des Live-Empfangs. Jeder hat
seine eigene Broker-Verbindung, Queue, Consumer und Puffer. Die Tests benötigen
keinen Webserver und sind noch kein vollständiger S3-Test über POST und SSE.

Vier Broker-Tests:

1. 20 Publishes auf chat.messages: beide Empfänger erhalten genau dieselben Inhalte
   in Reihenfolge; alle 20 Inhalte werden auch aus chat.persist gelesen und verglichen.
2. Zwei Queue-Bindings an chat.delivery, je ein Consumer mit Prefetch 1 und manueller
   Bestätigung. Queue-Eigenschaften werden über die Broker-Management-API gelesen.
   Nach Kontextende verschwindet die betroffene Queue innerhalb der 30-s-Testfrist;
   die andere sowie chat.persist und chat.dlq bleiben.
3. Kaputtes JSON, JSON null und fehlende Pflichtfelder blockieren keinen gültigen
   Nachbarn; reines JSON ohne Typheader funktioniert und doppelte IDs erscheinen
   im lokalen Puffer nur einmal. Beide Live-Queues werden leer.
4. Die Management-API schließt eine Verbindung von Broker-Seite. Der Empfangspuffer
   wird geleert und nach automatischer Wiederherstellung werden neue Nachrichten
   angenommen, ohne Neustart des Anwendungskontextes.

Zwei lokale Tests prüfen die Puffergrenze bei 10001 Nachrichten, die Reihenfolge
bei Dubletten, unabhängige Momentaufnahmen und die Invalidierung des Zustands.
Die 5000er-Pause-Prüfung und das Verhalten langsamer SSE-Clients sind noch offen;
Konfigurationsnachweise allein werden nicht als vollständiges S6 ausgegeben.

## Geprüft und korrigiert

Der erste Testentwurf verwendete Spring-Default-Properties für die Testcontainer-
Verbindung. application.yml hatte Vorrang und leitete die Tests fälschlich an
localhost:5672. Korrigiert auf Startargumente mit höherer Priorität; danach bestanden
alle vier Broker-Tests. Keine Änderung des Produktionsvertrags oder der Planreihenfolge.

Der bestehende Context-Smoke-Test startet den Live-Consumer ausdrücklich nicht,
weil er lediglich das Zusammensetzen der Beans prüft und keine externe Umgebung
voraussetzt. Echte Broker-Tests laufen verpflichtend separat im selben Maven-Lauf;
die Änderung überspringt keine Integrationstests.

Gezielter Lauf: `mvn -B -ntp -pl chat-service -Dtest=LiveDeliveryIntegrationTest test`:
4 Tests bestanden, keine Fehler/Fehlschläge/übersprungenen Tests.
Abschließender Root-Lauf: `mvn -B -ntp clean test`, BUILD SUCCESS am 13.11.2026
um 08:38:12 Uhr (Europe/Zurich), Dauer rund 91 s. 29 Tests bestanden (23 Chat-Service,
6 Batch-Writer), keine Fehler, Fehlschläge oder übersprungenen Tests. Vollständiges
lokales Protokoll: tmp/step2-root-tests.log (nicht versioniert).

## Technische Referenzen

RabbitMQ begrenzt mit max-length die wartenden, nicht die unbestätigten Lieferungen;
deshalb wird zusätzlich Prefetch begrenzt:
[Queue Length Limit](https://www.rabbitmq.com/docs/maxlength).
Die Wiederdeklaration verwendet Spring-Beans und RabbitAdmin:
[Configuring the Broker](https://docs.spring.io/spring-amqp/reference/3.2-SNAPSHOT/amqp/broker-configuration.html).
