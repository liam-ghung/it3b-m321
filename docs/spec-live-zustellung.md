# Spezifikation: Live-Zustellung

Stand: 12.11.2026. Geplanter Vertrag für Bewertung 2; noch nicht implementiert oder abgenommen.

## Ziel und Ausgangslage

Neue Nachrichten erscheinen bei allen verbundenen Clients ihres Raums, unabhängig
von der Chat-Service-Instanz und vom Fortschritt des Batch-Writers. Speicherung und
Live-Anzeige bleiben getrennte Aufgaben. Keine Erweiterung um Login, Gateway,
Benutzeroberflächen oder Raumverwaltung.

Belege im bestehenden Repository: MessageController unterstützt POST /messages und
/api/messages; MessageService.sendMessage vergibt UUID und Instant und publiziert
über RabbitTemplate; RabbitConfiguration deklariert chat.messages und die Bindung
zu chat.persist. MessageRepository.findLatest liest PostgreSQL, neueste zuerst.
PLANUNG.md 2.2–2.4 beschreibt flüchtige Live-Queues und getrennte Rückstauregeln.
Maßgeblich für diese Erweiterung sind die acht Szenarien des Auftrags Bewertung 2.

## Nachrichten- und HTTP-Vertrag

POST bleibt unverändert. Die vollständige Nachricht enthält id und roomId als UUID,
senderId, senderName und content als Text sowie sentAt als ISO-8601-Zeitpunkt.
Dasselbe JSON geht an Speicherung und Anzeige. Keine zusätzlichen Pflichtfelder.

GET /stream?roomId=<UUID> liefert HTTP 200 und Content-Type text/event-stream.
Ein Datenereignis hat folgende Form, abgeschlossen durch eine Leerzeile:

```text
id: <Nachrichten-UUID>
data: {"id":"…","roomId":"…","senderId":"…","senderName":"…","content":"…","sentAt":"…"}

```

JSON bleibt auf einer Zeile; Zeilenumbrüche im Inhalt werden JSON-konform maskiert.
Kein benanntes event-Feld erforderlich. Cache-Control: no-cache. Nach Registrierung
wird ein SSE-Kommentar gesendet, damit die Verbindung sichtbar geöffnet ist.
Weitere Kommentare als Heartbeat spätestens alle 10 s, ohne Nachrichten-ID.
Kommentare zählen nicht als Chatnachrichten. Fehlende/ungültige roomId ergibt 400.
Raumzugehörigkeit wird anhand roomId gefiltert, nicht über Benutzerberechtigungen.
Mehrere Verbindungen desselben Clients werden als eigenständige Streams behandelt.

## Broker-Topologie und S4

chat.messages bleibt der bestehende dauerhafte Fanout-Exchange mit chat.persist.
Für die Anzeige wird ein dauerhafter Fanout-Exchange chat.delivery ergänzt und
durch ein Exchange-to-Exchange-Binding an chat.messages angebunden. Ein POST
publiziert weiterhin nur einmal auf chat.messages. RabbitMQ verteilt zur Speicherung
und zum Anzeige-Exchange; kein zweiter Anwendungs-Publish mit Teilfehler-Risiko.

An chat.delivery hängen ausschließlich die Live-Queues. Bei zwei Instanzen sind
es genau zwei, je mit einem Consumer. Damit wird die im Auftrag zugelassene
Exchange-Bezeichnung chat.delivery benutzt und die S4-Zählung eindeutig gemacht.
chat.persist und chat.dlq werden weder umgebunden noch gelöscht. Dies ist die
gewählte Auslegung der Queue-Zählung; keine behauptete Freigabe des Lehrers.

Pro Instanz: chat.live.<zufällige UUID>, nicht dauerhaft, exklusiv und auto-delete.
Ein Consumer verarbeitet sequenziell. Verbindung und Listener werden beim normalen
Stop geschlossen, dadurch verschwindet die Queue innerhalb von 30 s. Nach Verlust
der Broker-Verbindung wird die Topologie automatisch neu deklariert; ein neuer
Queue-Name ist zulässig. Kein container_name für chat-service.

Live-Queue: Classic-Queue, maximal 999 Ready-Nachrichten, Prefetch 1 und manuelles
ACK. Damit sind höchstens 1000 Ready plus Unacked möglich. Überlauf verwirft die
ältesten Ready-Nachrichten (drop-head); TTL 30000 ms. Kein Zurückweisen des gesamten
Publishes wegen einer vollen Live-Queue. Die Speicherqueue bleibt unbeschränkt
durch diese Live-Regeln. Konfigurationswerte dürfen die Gesamtgrenze nicht erhöhen.

Der Live-Consumer bestätigt nach erfolgreicher Aufnahme in den begrenzten lokalen
Verteilungszustand, nicht erst nach Netzwerkzustellung an alle SSE-Clients. Ungültiges
JSON wird protokolliert und ohne Wiederholung abgelehnt; kein Endlos-Requeue.
Für die Anzeige ist dies keine dauerhafte Empfangsbestätigung des Endgeräts.

## Lokale Verteilung und Ressourcen

Ein einfacher zuständiger Dienst verwaltet Raumzuordnung, Wiederholungspuffer und
Client-Warteschlangen. Eine kurze gemeinsame Sperre schützt deren Veränderungen;
Netzwerkschreiben und Datenbankzugriffe laufen niemals unter dieser Sperre.
Jede Verbindung hat genau einen geordneten Sender und eine begrenzte Warteschlange
von 10000 Ereignissen. Höchstens 100 gleichzeitige SSE-Verbindungen pro Instanz;
weitere werden vor Öffnung mit 503 abgelehnt. Sender und Heartbeat-Aufgaben werden
bei Ende, Timeout oder Fehler beendet. Keine unbegrenzten Executor-Warteschlangen.

Ein voller Client-Puffer beendet nur diesen Stream. Er darf weder Broker-Consumer
noch andere Clients blockieren. Servlet-/Netzwerk-Timeouts und Abbruchverhalten
müssen mit einem nicht lesenden Client gemessen werden. Eine neue Verbindung mit
Cursor kann innerhalb des unten beschriebenen Fensters nachholen.

Die Instanz unterdrückt wiederholte Nachrichten-IDs innerhalb ihres begrenzten
Wiederholungspuffers. Sie ordnet nach ihrer sequenziellen Broker-Empfangsfolge.
S3 prüft je Absender sequenzielle POSTs an seine Instanz; Publishes werden dort
geordnet ausgeführt. Gleichzeitige Requests desselben Absenders ohne vorgegebene
Reihenfolge bekommen keine erfundene globale Ordnung. Es wird weder nach UUID
sortiert noch aus unterschiedlichen Systemuhren eine globale Ordnung abgeleitet.

## Neuverbindung: bewusster Wiederholungspuffer statt DB-Abhängigkeit

Jede Instanz bewahrt die letzten 10000 gültigen empfangenen Nachrichten in einer
geordneten Liste mit ID-Index auf, höchstens 120 s ab lokalem Empfang. Aufräumen
erfolgt bei Empfang/Registrierung und periodisch. Der Puffer erfasst alle Räume,
auch ohne gerade verbundene Clients. Grenzen gelten instanzweit, nicht pro Raum.
Diese Kapazität deckt den Szenarioumfang einschließlich 5000 Nachrichten in S6 ab.

Ohne Last-Event-ID erhält ein neuer Client nur Nachrichten ab seiner Registrierung.
Mit Last-Event-ID muss der Cursor im Puffer vorhanden sein und zum angefragten Raum
gehören. Unter derselben Sperre werden die nachfolgenden Nachrichten dieses Raums
in Empfangsreihenfolge in den neuen Client-Puffer gelegt und der Client registriert.
Erst danach beginnt sein Sender. Neue Ereignisse werden hinter den nachzuliefernden
angehängt. Der Cursor selbst wird nicht erneut gesendet. Einzige Eingangsquelle für
diesen Übergang ist der Live-Puffer: keine konkurrierende DB-Liste, keine Schlafpause.

Ungültige UUID im Header: 400. Unbekannter, abgelaufener, wegen Kapazität entfernter
oder raumfremder Cursor: 409 vor Öffnung des Streams mit Hinweis auf erneutes Laden
des Verlaufs. Keine stillschweigend unvollständige Nachlieferung. Nach Instanzneustart
oder Verlust von Broker-Nachrichten wird keine unbegrenzte Wiederholung zugesagt.
Bei erkanntem Verlust der Broker-Verbindung wird der lokale Wiederholungszustand
invalidiert und bestehende Streams werden geschlossen; danach Neuverbindung nötig.

S7 liegt mit 50 Nachrichten und 10 s innerhalb dieses Fensters. Solange jede Instanz
den Cursor und die Folge vollständig empfangen hat, funktioniert die Neuverbindung
auch an einer anderen solchen Instanz. Persistenzverzögerungen beeinflussen diesen
Weg nicht. PostgreSQL bleibt Quelle für langfristigen Verlauf über den bestehenden
GET-Endpunkt, nicht Voraussetzung der Live-Anzeige. Automatische Nachlieferung nach
Live-Queue-Überlauf oder Prozessneustart ist nicht Teil der zugesagten S7-Garantie.

## Szenarien und messbare Abnahme

Das geplante Skript scripts/acceptance-live.py wird während der Umsetzung erstellt.
Die folgenden Befehle sind der festgelegte Zielaufruf, noch kein vorhandener Test.
Python 3 ohne Zusatzpakete; Docker CLI und Hilfsclient im chat-net. Der Hilfsclient
wird nur für Prüfungen gestartet, veröffentlicht keine Ports und zählt nicht als
zusätzlicher dauerhafter Anwendungsdienst. Images/Werkzeuge werden dokumentiert.
Jede Instanz wird über ihre Container-Adresse einzeln angesprochen, nicht zufällig
über den gemeinsamen Compose-Dienstnamen.

```text
mvn clean test
docker compose up -d --build
python scripts/acceptance-live.py all
python scripts/acceptance-live.py S7
```

S1 läuft separat. all führt S2–S8 in der Reihenfolge des Auftrags auf demselben
Stack aus, ohne Queue-Purge oder Tabellen-Reset. Frische Raum-IDs pro Test.
Vor dem ersten Start .env aus .env.example kopieren, vorhandene Datei nicht ersetzen.
Bei Fehlern stellt finally pausierte/gestoppte Dienste wieder her. Ergebnisdatei
enthält pro Szenario Messwerte und Erfolg/Fehler, keine vorweggenommenen Ergebnisse.

| Szenario | Ablauf und messbarer Erfolg |
|---|---|
| S1 | Root-Maven-Lauf: alle Module erfolgreich, keine ausgelassenen Writer-/Live-Integrationstests. Docker muss laufen. |
| S2 | Frischer Klon des versionierten Kandidaten, nur .env.example als Vorlage; Compose-Build erfolgreich, vier Dienste laufen, HTTP erreichbar im Netz, keine Host-Portbindungen in config und inspect. |
| S3 | Auf zwei Chat-Instanzen skalieren; je ein A-Client, zusätzlich B-Client. Je 200 POSTs über jede Instanz, pro Absender sequenziell. Beide A-Clients erhalten exakt die 400 zurückgegebenen IDs einmal; Absenderfolgen stimmen; B erhält keine. |
| S4 | Vorher an chat.delivery genau zwei Live-Queues mit je einem Consumer. Auf eine Instanz reduzieren; ab Start des Scale-down höchstens 30 s bis nur noch eine Live-Queue vorhanden ist. chat.persist und chat.dlq bleiben. |
| S5 | SSE öffnen, Writer und PostgreSQL stoppen, 100 POSTs senden. Alle 100 IDs spätestens 5 s nach Abschluss des letzten erfolgreichen POSTs empfangen; zusätzlich Latenz jeder Nachricht ab POST-Beginn protokollieren. Dienste starten und alle IDs in SQL nachweisen. |
| S6 | Wieder zwei Instanzen, SSE-Client an gesunder Instanz; genau die andere über ihre Container-ID pausieren. 5000 POSTs an die gesunde Instanz. Management-Abfragen messen Ready+Unacked währenddessen regelmäßig (Zielintervall 100 ms); Maximalwert höchstens 1000, zusätzlich Konfiguration/Prefetch prüfen. Gesunder Client erhält alle IDs genau einmal. Fortsetzen und neue Testnachricht an beiden Instanzen nachweisen. |
| S7 | Client empfängt Ankernachricht und trennt. 50 sequenzielle Nachrichten senden; nach 10 s wieder mit Last-Event-ID verbinden. Genau diese 50 IDs in Reihenfolge, kein Anker, keine Duplikate; zusätzliche neue Nachricht erscheint live. |
| S8 | Alle Java-Dateien in chat-service/ auf Streams und fehlende deutsche Klassen-/Methodenkommentare prüfen, Kommentare manuell auf Sinn kontrollieren. git ls-files -- .env liefert nichts. |

Bei S3/S6 hat das PDF keine eigene numerische Zustellfrist. Die Prüfungen verwenden
eine endliche Diagnosefrist von 60 s nach dem letzten POST, ohne sie als Lehrerfrist
auszugeben. S5-Speichern nach Neustart wird ebenfalls höchstens 60 s beobachtet.
Queue-Messungen sind Stichproben; die strukturelle Grenze aus Ready-Limit und
Prefetch wird zusätzlich geprüft und nicht durch bloßes Polling ersetzt.

## Automatisierte Tests und Dokumentation

chat-service/src/test/ enthält Tests mit echtem RabbitMQ und zwei unabhängig
konfigurierten Anwendungsinstanzen, keine reine Mock-Verteilung. Pflicht: S3 und S7.
Zusätzlich: Raumfilter, SSE-Format, Verbindungsbereinigung, volle Client-Puffer,
Queue-Löschung, Replay/Live-Rennen, Deduplizierung, Cursor-Ablauf/Unbekanntheit und
DB-Unabhängigkeit des Wiederholungspfads. Testgrößen dürfen für fokussierte
Einzeltests kleiner sein; vollständige Zahlen gelten für die Gesamtabnahme.

Neue konfigurierbare Werte und ihre Grenzen werden in .env.example dokumentiert,
der README-Stand wird aktualisiert. Vorgegebene Höchstgrenzen werden validiert.
Die Implementierung folgt docs/plan-live-zustellung.md. Ergebnisse, Freigaben und
Code-Review werden nur als erfolgt dokumentiert, wenn sie tatsächlich erfolgt sind.
