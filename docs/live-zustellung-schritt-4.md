# Schritt 4: Zwei Instanzen und unabhängiger Speicherweg

Am 13.11.2026 nach dem vorab gepushten Plan umgesetzt. Dieser Schritt ergänzt
Tests; Änderungen am Produktionscode waren nicht nötig.

## Was geprüft wird

`LiveMultiInstanceTest` startet zwei vollständige Spring-Boot-Anwendungen mit
verschiedenen HTTP-Ports und einen echten gemeinsamen RabbitMQ-Testcontainer.
Jede Anwendung hat ihre eigene Live-Queue, ihren Puffer und ihre SSE-Verbindungen.
Je ein Client für Raum A hängt an jeder Instanz, ein weiterer für Raum B an der
zweiten. Zwei parallele Absender senden je 200 sequenzielle HTTP-POSTs an ihre
fest zugewiesene Instanz. Beide A-Clients müssen alle 400 IDs genau einmal erhalten.
Jedes vollständige Nachrichtenobjekt muss der POST-Antwort entsprechen; zusätzlich
wird die Reihenfolge je Absender verglichen. Raum B und zusätzliche Ereignisse
nach den 400 erwarteten Nachrichten werden ausdrücklich geprüft.

Dieser Test läuft automatisch im Root-Maven-Lauf. PostgreSQL wird dafür nicht
gestartet. Er ersetzt nicht den separaten Compose-Test mit zwei JVM-Prozessen.

## Wiederholbare Docker-Prüfungen

Voraussetzungen: Docker, Python 3 auf dem Host, vorhandene `.env` nach
`.env.example`. Keine Python-Zusatzpakete. Zuerst den aktuellen Code bauen:

```text
docker compose up -d --build --scale chat-service=2
python scripts/acceptance-live.py S3
python scripts/acceptance-live.py S5
```

Ein abweichender Projektname lässt sich mit `--project NAME` angeben. Die Datei
ergänzt das bisherige `scripts/acceptance.py` für Bewertung 1, ersetzt es nicht.
Aktuell sind ausschließlich S3 und S5 verfügbar; `all` und die weiteren Szenarien
werden erst in ihren geplanten Schritten ergänzt.

Das Skript stellt zwei Chat-Instanzen und einen Writer sicher. Ein kurzlebiger
Hilfscontainer mit `python:3.13-alpine` läuft im `chat-net`, ohne veröffentlichte
Ports. Er erhält nur das Prüfskript als schreibgeschützte Datei, weder `.env` noch
Docker-Socket. Er spricht die aus `docker inspect` gelesenen IP-Adressen der beiden
Instanzen direkt an. Ein Leser pro SSE-Verbindung erfasst Nachrichten und monotone
Empfangszeit; die POST-Messung erfolgt im gleichen Prozess und mit derselben Uhr.
Der Hilfscontainer wird anschließend entfernt. Testdaten bleiben erhalten; keine
Queue wird geleert, keine Tabelle oder kein Volume zurückgesetzt.

S3 prüft dieselben 400 Nachrichten wie der Java-Test, diesmal mit zwei getrennten
Containern. Die Diagnosefrist beträgt gemeinsam 60 s nach dem letzten POST.

S5 öffnet zuerst SSE, stoppt dann Writer und PostgreSQL und kontrolliert, dass beide
nicht mehr laufen. Erst danach werden 100 POSTs gesendet. Alle IDs müssen spätestens
5 s nach Abschluss des letzten erfolgreichen POSTs eingetroffen sein. Zusätzlich
wird für jede ID die Zeit vom Beginn ihres POSTs bis zum SSE-Empfang gespeichert.
Eine negative Differenz zwischen letzter Zustellung und letztem POST-Abschluss wäre
zulässig: SSE kann bereits vor der HTTP-Antwort eintreffen.

Im `finally` werden beide gestoppten Dienste auch bei Testfehlern wieder gestartet.
Nach dem Neustart vergleicht SQL die komplette ID-Liste des frischen Testraums mit
den 100 POST-Antworten, nicht bloß eine globale Zeilenanzahl. Dafür gilt eine weitere
Diagnosefrist von 60 s. Der Stack bleibt danach mit zwei Chat-Instanzen und einem
Writer aktiv.

## Tatsächlich gemessene Ergebnisse

- Gezielter Maven-Test: BUILD SUCCESS, 1 Test, keine Fehler oder ausgelassenen Tests;
  beendet am 13.11.2026 um 09:00:53 Uhr (33,199 s).
- Compose-Build und Start mit zwei Chat-Instanzen erfolgreich, keine Host-Ports.
- S3: beide A-Clients jeweils 400 vollständige Nachrichten, 400 verschiedene IDs,
  richtige Reihenfolge je Absender, keine zusätzlichen Nachrichten; B erhielt 0.
- S5: alle 100 Nachrichten live; letzte Zustellung 0,000778 s nach Abschluss des
  letzten POSTs, also innerhalb der 5-s-Grenze.
- S5: höchste Einzellatenz ab POST-Beginn 0,005454 s, Mittelwert 0,003212 s.
- S5: nach Neustart alle 100 IDs in PostgreSQL nachgewiesen. SQL-Wartezeit nach
  Rückkehr des Startbefehls: 8,344 s.
- Abschließender Root-Maven-Lauf `mvn -B -ntp clean test`: BUILD SUCCESS am
  13.11.2026 um 09:05:28 Uhr, Dauer 2:45 min. Alle 37 Tests bestanden
  (31 chat-service, 6 batch-writer); keine Fehler, Fehlschläge oder übersprungenen Tests.

Die lokalen Rohberichte liegen unter `tmp/live-S3-result.json` und
`tmp/live-S5-result.json`. Sie enthalten Erfolg/Fehler, Raum, IDs, Instanzadressen
und für S5 alle 100 Latenzen. `--report PFAD` erlaubt einen anderen Ausgabeort.
Die Werte sind Ergebnisse dieses lokalen Laufs, keine garantierten Antwortzeiten.
Auf diesem Rechner wurde mangels Python-Eintrag im PATH der vorhandene gebündelte
Python-Interpreter verwendet. Das Skript selbst benötigt nur die Standardbibliothek.

## Prüfung und Grenzen

Beim ersten Start des Hilfscontainers lag das Skript direkt unter `/`; der Zugriff
auf `parents[1]` schlug deshalb fehl. Die Pfadbestimmung verwendet nun
`parent.parent`, die auch dort funktioniert. S3 wurde anschließend erfolgreich
wiederholt; S5 lief ebenfalls erfolgreich. Dies betraf nur das Prüfskript.

Keine Vertragsänderung, kein zweiter Publish und keine Datenbankabfrage im Live-Weg.
Die Tests bestätigen die bereits gebaute Trennung: RabbitMQ verteilt eine Kopie
sofort an die Live-Queues und hält eine weitere Kopie für den späteren Writer bereit.
S4, S6, S7 und die vollständige frische Abnahme sind dadurch noch nicht abgeschlossen.
