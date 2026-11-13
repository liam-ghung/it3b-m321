# Schritt 3: SSE und Raumfilterung

Datum: 13.11.2026. Umsetzung des vorab gepushten Schritts 3.
Die vollständige Mehrinstanz-Abnahme, Lasttests und Last-Event-ID-Nachlieferung
folgen weiterhin in ihren geplanten Schritten.

## Nachrichtenweg

```text
POST /messages -> chat.messages -> chat.delivery -> instanzeigene Live-Queue
  -> LiveMessageListener -> LiveMessageBuffer.accept
  -> Warteschlangen der Clients im passenden Raum
  -> je ein HTTP-Worker -> id: und data: am SSE-Stream
```

LiveStreamController öffnet GET /stream?roomId=<UUID>. Die Antwort trägt
text/event-stream;charset=UTF-8 und Cache-Control: no-cache. Fehlende/ungültige
Raum-ID ergibt 400, die 101. registrierte Verbindung 503 vor Stream-Beginn.
Ohne Cursor gibt es nur Nachrichten ab Registrierung; vorhandener Verlauf wird
nicht erneut ausgegeben. Last-Event-ID ist noch nicht implementiert und darf in
diesem Zwischenschritt nicht als verfügbare Wiederaufnahmefunktion verstanden werden.

LiveMessageBuffer verwaltet Empfangsfolge und Clients je Raum unter derselben kurzen
Sperre. Annahme und Registrierung sind dadurch eindeutig geordnet. Das Verteilen
verwendet ausschließlich nicht blockierendes offer in die begrenzten Client-Puffer.
Die bereits vorhandene ID-Deduplizierung gilt auch für die SSE-Verteilung.
Unter der gemeinsamen Sperre wird weder JSON serialisiert noch ins Netzwerk geschrieben.

## Sender und Ressourcen

LiveStreamClient hält höchstens 10000 wartende Nachrichten. Bei vollem Puffer wird
nur dieser Client geschlossen und aus der Raumverwaltung entfernt. Ein Abschluss-
Signal weckt einen wartenden Sender. close führt keinen Netzwerkzugriff aus.
Netzwerkschreiben übernimmt pro Verbindung ein StreamingResponseBody-Callback im
begrenzten MVC-Executor. Das ist HTTP-Streaming, keine Java-Stream-Pipeline.

Es gibt höchstens 100 registrierte Verbindungen und 100 aktive Sender-Threads.
Auch die Executor-Warteschlange ist auf 100 begrenzt: Bei kurzen Übergängen zwischen
beendenden und neu registrierten Verbindungen entstehen keine unbegrenzten Tasks.
Ein Sender schreibt zuerst den Kommentar `: verbunden`, anschließend je Nachricht
`id: <UUID>` und eine einzelne JSON-`data:`-Zeile, abgeschlossen durch eine Leerzeile.
Der gemeinsame ObjectMapper maskiert Zeilenumbrüche und Anführungszeichen im Text.
Jedes Ereignis wird geflusht. Keine zusätzlich benannten SSE-Events.

Der gleiche Sender sendet regelmäßig Heartbeat-Kommentare ohne ID, auch ohne
Nachrichten. Ein leerer Puffer wartet höchstens 5 s; bei laufenden Nachrichten
wird ebenfalls nach 5 s seit dem letzten Heartbeat ein Kommentar eingeplant.
Die Kommentare zählen nicht als Chatnachrichten. Scheduling und langsames Schreiben
sind keine harte Echtzeitgarantie; ein dauerhaft blockierter Socket wird begrenzt.

Tomcat NIO erhält connection-timeout: 5s, das auch für blockierende Socket-Writes
verwendet wird. Ein nicht lesender Client hält damit keinen Worker unbegrenzt fest.
Diese Connector-Einstellung begrenzt ebenfalls das Lesen von HTTP-Anfragen und ist
keine reine SSE-Option. Eine normale SSE-Verbindung hat einen asynchronen
Gesamttimeout von 30 Minuten. Danach ist eine neue Verbindung erforderlich; sichere
Nachlieferung bei Neuverbindung wird erst mit Schritt 6 zugesagt.

Die Freigabe erfolgt im finally des Senders und zusätzlich nach der letzten
HTTP-Dispatch-Phase. Das deckt Fehler/Timeout vor dem Start des Callbacks mit ab.
Aufräumen ist idempotent. Bei Broker-Verbindungsverlust und beim Schließen des
Spring-Kontextes werden die Streams invalidiert; beim Herunterfahren geschieht
das vor dem Warten auf offene HTTP-Anfragen. Heartbeats erkennen normale
Client-Abbrüche auch ohne neue Chatnachrichten.

## Tests

LiveStreamIntegrationTest startet einen echten HTTP-Server auf zufälligem Port
und echtes RabbitMQ. Keine SSE-Attrappe. Fünf Prüfungen:

1. Zwei HTTP-Clients in Raum A und einer in B; fünf echte POSTs laufen über RabbitMQ.
   Beide A-Clients erhalten exakt das jeweilige POST-Ergebnis mit passender ID und
   vollständigem JSON in Reihenfolge, ohne zusätzliche Ereignisse. B erhält keines.
   Anführungszeichen, Umlaute und ein Textzeilenumbruch bleiben korrekt erhalten.
2. Eine Nachricht vor Registrierung wird nicht wiederholt; die nächste wird live zugestellt.
3. Fehlender/falscher Raum ergibt 400. Nach 100 direkt registrierten Slots wird
   eine zusätzliche echte HTTP-Anfrage mit 503 abgewiesen. Dieser Kapazitätstest
   behauptet nicht, 100 gleichzeitig lesende TCP-Clients simuliert zu haben.
4. Abbruch eines echten HTTP-Clients: Raumregistrierung und aktiver Sender werden
   innerhalb der 15-s-Testfrist entfernt, ohne eine weitere Chatnachricht zu senden.
5. Ein echter TCP-Client mit kleinem Empfangspuffer liest nach den HTTP-Headern
   nichts mehr. 64 große Nachrichten für seinen Raum füllen den Socket-Schreibweg.
   Ein gesunder Client in einem anderen Raum erhält weiterhin seine Nachricht
   innerhalb von 3 s. Der hängende Client und sein Sender verschwinden innerhalb
   von 15 s, bevor der Test seinen Socket schließt. Die großen Nachrichten werden
   dafür direkt dem Verteilungszustand übergeben; es ist kein Broker-Lasttest.

Zwei lokale Tests prüfen einen gezielt gefüllten Client-Puffer und das Beenden aller
registrierten Clients beim Invalidieren des Empfangszustands.
Nach jedem HTTP-Test werden außerdem registrierte Verbindungen, aktive Sender und
wartende Executor-Aufgaben auf null geprüft.

Gezielter Lauf: `mvn -B -ntp -pl chat-service -Dtest=LiveStream*Test test`:
7 Tests bestanden, keine Fehler, Fehlschläge oder übersprungenen Tests.
Abschließender Root-Lauf: `mvn -B -ntp clean test` am 13.11.2026,
BUILD SUCCESS um 08:53:14 Uhr nach 2 Minuten. Alle 36 Tests bestanden
(30 im chat-service, 6 im batch-writer), keine Fehler, Fehlschläge oder
übersprungenen Tests. Dieser Lauf enthält auch die abschließenden Änderungen
an Heartbeats und dem Aufräumen beim ContextClosedEvent.

## Entscheidungen und Prüfung

StreamingResponseBody mit explizitem begrenztem Executor wurde gewählt, damit genau
ein Sender die JSON-Rahmen schreibt und blockierende Socket-Zugriffe vollständig vom
Broker getrennt bleiben. Ein direktes Schreiben aus dem RabbitMQ-Listener wurde
vermieden, weil ein langsamer Client sonst den Empfang anhalten könnte.
Keine Vertrags- oder Reihenfolgeänderung im Plan; die vorhandene lokale Übergabestelle
wurde wie vorgesehen um Raumregistrierung ergänzt. Keine neuen Umgebungsvariablen.

Der bestehende reine MessageController-Webschichttest schließt die separate
LiveStreamConfiguration aus. Er prüft weiterhin nur den Nachrichtencontroller;
die SSE-Konfiguration wird in den echten HTTP-Integrationstests geprüft.
Zusätzlich wurde das Schließen beim ContextClosedEvent ergänzt, damit offene
Streams das geordnete Herunterfahren nicht bis zum Gesamttimeout verzögern.

## Referenzen

Die Wahl des begrenzten asynchronen Executors folgt den
[Spring-Hinweisen zu asynchronen HTTP-Antworten](https://docs.spring.io/spring-framework/reference/web/webmvc/mvc-ann-async.html).
Tomcats NIO-Socket übernimmt den Connection-Timeout als Write-Timeout; geprüft am
[Tomcat-Quelltext](https://github.com/apache/tomcat/blob/10.1.x/java/org/apache/tomcat/util/net/NioEndpoint.java)
und zusätzlich durch den echten nicht lesenden TCP-Client im Integrationstest.
