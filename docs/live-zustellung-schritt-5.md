# Schritt 5: Rückstau und Wiederanlauf unter Pause

Am 13.11.2026 gemäß vorab gepushtem Plan umgesetzt. Der Produktionscode musste
nicht geändert werden: Die bestehenden Grenzen haben im Versuch funktioniert.

## Reproduzierbarer Test

Mit Docker, Python 3 und vorhandenem `.env`:

```text
docker compose up -d --build --scale chat-service=2
python scripts/acceptance-live.py S6
mvn -B -ntp clean test
```

Das Prüfskript stellt zwei Chat-Instanzen und einen Writer sicher und startet
einen kurzlebigen Python-Hilfscontainer im `chat-net`, ohne Host-Ports. Es ordnet
die Live-Queue anhand des Consumer-Peers eindeutig der zweiten Container-IP zu.
Vor dem Versuch werden ein Consumer, manuelles ACK, Prefetch 1, Classic-Queue,
999 Ready, drop-head, TTL 30000 ms, exclusive/auto-delete und nicht dauerhaft geprüft.

Danach wird SSE an der gesunden Instanz geöffnet. Die zweite Instanz wird gezielt
mit `docker pause <Container-ID>` angehalten; Docker muss `Paused=true` melden.
Das ist dieselbe Container-Pause wie bei Compose, gezielt für genau eine Replik:
`docker compose pause chat-service` würde beide Instanzen anhalten. Es wird weder
der Broker noch der Writer pausiert. Diese Auswahl entspricht der in der
Spezifikation festgelegten gezielten Container-ID.

Ein Absender sendet 5000 sequenzielle echte POSTs an die gesunde Instanz. Der dortige
SSE-Client muss alle zurückgegebenen IDs genau einmal und in Absenderreihenfolge
erhalten; auch der vollständige JSON-Inhalt wird verglichen. Zusätzliche Ereignisse
führen zum Fehler. Die Diagnosefrist ist 60 s nach dem letzten POST.

Ein zweiter Thread fragt währenddessen die Queue im Zielintervall von 100 ms ab.
Jede Stichprobe enthält Ready, Unacked und ihre Summe. Die Messung beginnt vor der
Pause und läuft durch den gesamten Sende-/Empfangstest bis zur Anforderung der
Wiederaufnahme. Ein tatsächlich gefüllter Rückstau muss sichtbar sein; ein leeres
oder fehlendes Messergebnis gilt nicht als Erfolg. Danach wird die Instanz
fortgesetzt und `Paused=false` geprüft. In einem frischen Raum werden Streams an
beiden Instanzen geöffnet und drei neue Nachrichten gesendet. Beide müssen diese
vollständig, in Reihenfolge und ohne Duplikate erhalten.

Auch bei Fehlern hebt `finally` die Pause auf und entfernt den eigenen Testcontainer.
Queues, Tabellen und Volumes werden nicht geleert. Zugangsdaten gelangen nur über
stdin an den Testclient, nicht in Prozessargumente oder den Ergebnisbericht.

## Tatsächliche Ergebnisse

S6 im lokalen Compose-Stack bestanden:

| Messung | Ergebnis |
|---|---:|
| POSTs an gesunde Instanz | 5000 |
| Vollständige SSE-Nachrichten am gesunden Client | 5000, genau einmal |
| Sendedauer | 5,567 s |
| Management-Stichproben | 92 |
| Letzte Stichprobe ab Messbeginn | 9,125025 s |
| Größter Abstand zweier Stichproben | 0,107555 s |
| Maximal Ready | 999 |
| Maximal Unacked | 1 |
| Maximal Ready + Unacked in derselben Stichprobe | 1000 |
| Neue Nachrichten nach Wiederaufnahme | 3 an jedem der beiden Clients |

Rohbericht: `tmp/live-S6-result.json`, mit allen 5000 IDs, Queue-Namen,
Konfiguration, Instanzadressen und allen Messpunkten. Ein anderer Speicherort
kann mit `--report PFAD` gewählt werden. Die Werte sind lokale Messergebnisse.

## Warum die Grenze auch zwischen Stichproben gilt

Die Messung allein ist kein lückenloser Beweis: Die RabbitMQ-Management-Statistik
wird standardmäßig nur alle fünf Sekunden aktualisiert. Häufigeres Abfragen macht
daraus keine entsprechend häufig erneuerten Broker-Messwerte.
Siehe [RabbitMQ: Statistikintervall](https://www.rabbitmq.com/docs/management#statistics-interval).

Deshalb prüft der Test zusätzlich die tatsächlich deklarierten Queue-Argumente und
den Consumer: maximal 999 Ready plus maximal eine unbestätigte Lieferung bei genau
einem Consumer mit Prefetch 1 ergeben höchstens 1000. drop-head verwirft bei Überlauf
alte wartende Live-Nachrichten. Die Speicherqueue erhält diese Live-Begrenzung nicht.
Es wird nicht behauptet, dass die pausierte Instanz danach alle alten 5000 Nachrichten
nachholt; getestet werden die vollständige Zustellung an der gesunden Instanz und
neue Nachrichten nach Wiederaufnahme.

## Lokale Puffer und Maven

Ein zusätzlicher Test füllt den tatsächlichen Client-Puffer mit 10000 Nachrichten.
Bis dahin bleibt der Client offen. Die 10001. Nachricht schließt und entfernt nur
diesen Client. Ein anderer Raum bleibt nutzbar, der lokale Wiederholungspuffer bleibt
auf 10000 Einträge begrenzt. Der bestehende Puffer-Test prüft außerdem das Verdrängen
des ältesten Eintrags und die ID-Deduplizierung. Dies sind gezielte lokale Tests;
es wird keine Heap-Messung innerhalb des pausierten Containers behauptet.

Root-Maven-Lauf `mvn -B -ntp clean test`: BUILD SUCCESS am 13.11.2026 um
09:12:54 Uhr, Dauer 2:29 min. Alle 38 Tests bestanden (32 chat-service,
6 batch-writer), keine Fehler, Fehlschläge oder übersprungenen Tests.
Die bisherigen Compose-Szenarien S3 und S5 wurden nach der Erweiterung des
gemeinsamen Prüfskripts erneut erfolgreich ausgeführt. Abschließend meldeten
beide Chat-Container `paused=false` und `running=true`.
Keine Vertrags- oder Planänderung. S7 (Nachlieferung mit Last-Event-ID) ist weiterhin
Schritt 6; die abschließende Gesamtabnahme folgt in Schritt 7.
