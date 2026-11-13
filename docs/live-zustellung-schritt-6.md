# Schritt 6: Nachlieferung nach Wiederverbindung

Umsetzung vom 13.11.2026 gemäß vorab gepushtem Plan. Kein neuer Dienst und keine
neuen Umgebungsvariablen. Die bisherigen Speicher- und Live-Wege bleiben erhalten.

## Einfach erklärt

Der Client merkt sich die ID seiner letzten empfangenen Nachricht. Nach einem
Verbindungsabbruch schickt er diese ID im Header `Last-Event-ID` mit. Der Server
sucht sie in seinem kurzen lokalen Nachrichtenpuffer und liefert alle danach
empfangenen Nachrichten desselben Raums. Der Anker selbst kommt nicht noch einmal.
Anschließend läuft dieselbe Verbindung mit neuen Live-Nachrichten weiter.

```text
GET /stream?roomId=<Raum-UUID>
Last-Event-ID: <zuletzt-empfangene-Nachrichten-UUID>
```

Ohne Header gibt es wie bisher nur neue Nachrichten. Ein fehlerhafter Header
(auch leer oder verkürzte UUID) ergibt 400. Ein unbekannter, abgelaufener,
verdrängter oder raumfremder Anker ergibt 409 vor Beginn des Streams. Der Client
muss dann den Verlauf neu laden. Es wird keine vollständige Nachlieferung über
das festgelegte Pufferfenster hinaus versprochen.

## Warum beim Übergang nichts verloren geht

`LiveStreamController.parseCursor` prüft die UUID und übergibt sie zusammen mit
der Raum-ID an `LiveMessageBuffer.register`. Dort passiert unter derselben Sperre
wie beim Broker-Empfang:

1. Abgelaufene Einträge entfernen und Anker sowie Raum prüfen.
2. Alle folgenden Einträge dieses Raums in Empfangsreihenfolge in den Client-Puffer legen.
3. Den Client für kommende Live-Nachrichten registrieren.

Während dieser kurzen Schritte kann `accept` keine Nachricht dazwischenschieben.
Eine Nachricht liegt deshalb entweder bereits im nachzuliefernden Abschnitt oder
wird nach Registrierung hinten an den Client-Puffer angehängt. Es gibt keinen
separaten DB-Verlauf, dessen Ergebnisse mit Live-Ereignissen zusammengeführt werden
müssten. Schreiben ins Netzwerk beginnt erst danach im vorhandenen HTTP-Worker,
außerhalb der Sperre. Auch während des tatsächlichen Sendens neuer Nachrichten
bleibt die Reihenfolge des gemeinsamen Client-Puffers erhalten.

Der Anker gehört zum maximal 10000 Einträge großen Wiederholungspuffer. Dadurch
sind höchstens 9999 Folgeereignisse vorzubefüllen; sie passen in den 10000er-Puffer
des Clients. Für danach zu langsame Clients gilt unverändert die Überlaufregel.
Die Aufbewahrung bleibt auf 120 s begrenzt und wird auch bei Registrierung geprüft.
Eine monotone Uhr verhindert Einflüsse durch Änderungen der Systemzeit. Im Test
kann diese Uhr gezielt weitergestellt werden, ohne zwei Minuten warten zu müssen.

## Tests

- `LiveReplayBufferTest`: Ablauf exakt an der 120-s-Grenze, fremder/unbekannter
  Cursor, Invalidierung, Verdrängung durch Kapazität. Ein deterministischer Test
  registriert den Client, nimmt weitere Nachrichten und eine Dublette entgegen
  und startet erst dann den Sender: Nachlieferung steht vor live, keine Dublette,
  kein Anker, kein fremder Raum.
- `LiveStreamIntegrationTest`: echtes RabbitMQ und HTTP, explizit unerreichbare
  Datenbank. S7 mit Anker, Trennung, 50 POSTs, 10 s Wartezeit, Wiederverbindung
  und neuer Live-Nachricht. Ein weiterer Test sendet während der Wiederverbindung
  parallel 50 zusätzliche POSTs zu 30 bereits verpassten Nachrichten und vergleicht
  alle 80 vollständigen Nachrichten in Reihenfolge. HTTP-Fehler 400/409 werden
  zusätzlich geprüft; fehlerhafte Anfragen hinterlassen keine Registrierung.
- `LiveMultiInstanceTest`: Anker an Instanz 1 empfangen, trennen, 50 POSTs,
  10 s warten, an Instanz 2 nachliefern lassen und anschließend live weiterlesen.
  Beide vollständigen Spring-Anwendungen haben getrennte HTTP-Ports und einen
  gemeinsamen echten Broker; die Datenbank ist ausdrücklich unerreichbar.

## Compose-Abnahme

```text
docker compose up -d --build --scale chat-service=2
python scripts/acceptance-live.py S7
mvn -B -ntp clean test
```

S7 öffnet zunächst SSE. Der Host stoppt Writer und PostgreSQL und prüft deren
Zustand, bevor der Hilfsclient den Anker und danach 50 Nachrichten sendet.
Nach zehn Sekunden verbinden zwei Clients mit demselben Cursor: einer an der
ursprünglichen, einer an der anderen Instanz. Beide müssen genau die 50 Nachrichten
erhalten, danach dieselbe neue Live-Nachricht, ohne Anker oder Duplikate.
Damit wird auch fehlende DB-Sichtbarkeit geprüft: Währenddessen kann keine der
Testnachrichten gespeichert oder aus PostgreSQL nachgeladen werden.
Der Host startet beide Dienste im `finally` wieder und weist anschließend alle
52 IDs in SQL nach. Es werden keine Tabellen, Queues oder Volumes geleert.

## Ergebnisse und Grenzen

Gezielter Lauf von `LiveReplayBufferTest` und `LiveStreamIntegrationTest`:
11 Tests bestanden, keine Fehler/Fehlschläge/übersprungenen Tests,
BUILD SUCCESS um 09:23:47 Uhr (59,032 s).
Root-Maven-Lauf `mvn -B -ntp clean test`: BUILD SUCCESS am 13.11.2026 um
09:27:28 Uhr nach 2:58 min. Alle 45 Tests bestanden (39 chat-service,
6 batch-writer), keine Fehler, Fehlschläge oder übersprungenen Tests.

Compose-S7 bestanden: Auf beiden Instanzen jeweils genau 50 Nachrichten
nachgeliefert und danach eine neue Nachricht live empfangen, keine Duplikate
oder erneuter Anker. Writer und PostgreSQL waren währenddessen gestoppt.
Nach Neustart wurden alle 52 IDs nachgewiesen; die SQL-Wartezeit nach Rückkehr
des Startbefehls betrug 6,140 s. Beide Chat-Instanzen, Writer und Datenbank
laufen wieder. Rohbericht mit IDs und Messwerten: `tmp/live-S7-result.json`.

Der Puffer ist lokal und begrenzt. Ein Wechsel auf eine andere Instanz funktioniert
nur, wenn diese den Anker und die folgenden Nachrichten ebenfalls empfangen hat.
Nach Neustart, Verbindungsverlust zum Broker oder verworfenen Live-Nachrichten ist
keine unbegrenzte Wiederholung zugesagt. Bei erkanntem Broker-Verlust werden Puffer
und offene Streams wie bisher invalidiert. Die Speicherqueue bleibt unabhängig.
Keine Vertrags- oder Planänderung. Die vollständige Gesamtabnahme und der
Abgabe-Tag `bewertung-2` gehören weiterhin zu Schritt 7.
