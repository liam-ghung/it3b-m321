# Gesamtabnahme Bewertung 2

Prüfdatum: 13.11.2026. Die technischen Abnahmekriterien stehen in
`spec-live-zustellung.md`; der vorab gepushte Plan in `plan-live-zustellung.md`.

## Reproduzieren

Frischen Klon verwenden, `.env.example` nach `.env` kopieren. Voraussetzungen:
Java 21, Maven, Docker mit Compose, Python 3 ohne Zusatzpakete.

```text
mvn clean test
docker compose up -d --build
python scripts/acceptance-live.py all
```

S2–S8 laufen in dieser Reihenfolge auf demselben Stack ohne Datenbereinigung.
S1 ist der Root-Maven-Lauf. Der Hilfsclient nutzt `python:3.13-alpine` intern im
`chat-net`, ohne Host-Ports. Der Host benötigt Docker CLI und Git.
`--project NAME` wählt einen Compose-Projektnamen, `--report PFAD` die Ergebnisdatei.
Der feste Netzwerkname `chat-net` bedeutet: keinen zweiten unabhängigen Stack
gleichzeitig an diesem Netz betreiben. Vor dem frischen Test vorhandenen Stack
geordnet herunterfahren; Volumes nicht löschen.

## Prüfumfang

| Szenario | Nachweis |
|---|---|
| S1 | Alle Maven-Module, echte RabbitMQ- und PostgreSQL-Testcontainer, kein übersprungener Test. |
| S2 | Separater frischer Klon des committed Kandidaten, nur `.env.example` als Vorlage; Build. Skript prüft vier Dienste, UP und keine Host-Portbindungen. |
| S3 | Zwei Instanzen, je 200 POSTs, beide A-Clients genau 400 vollständige Nachrichten, Absenderfolge und keine Duplikate, B leer. |
| S4 | Genau zwei Live-Queues mit je einem Consumer; Scale-down ab Befehlsbeginn höchstens 30 s bis zu einer Queue. Speicherqueues bleiben. |
| S5 | Nach S4 eine Instanz; Writer und DB aus, 100 Nachrichten innerhalb der festgelegten 5-s-Frist. Danach SQL-ID-Vergleich. |
| S6 | Wieder zwei Instanzen, eine gezielt pausiert; 5000 live, maximal 1000 Ready+Unacked und strukturelle Begrenzung. Nach Fortsetzen neue Nachrichten an beiden. |
| S7 | 50 fehlende Nachrichten nach 10 s auf beiden Instanzen, danach live; zusätzlich Speicherweg aus und später alle 52 IDs in SQL. |
| S8 | Alle Java-Dateien von chat-service einschließlich Tests gelesen; heuristische Kommentarprüfung und Stream-Suche, `.env` nicht versioniert. |

Keine neuen Live-Umgebungsvariablen. `.env.example` dokumentiert die festen Grenzen.
Die technischen Grenzfälle und Messgrenzen stehen in den Berichten der Schritte 3–6.
Management-Abfragen sind Stichproben mit zeitverzögerten Broker-Statistiken;
deshalb werden Queue-Limit und Prefetch zusätzlich geprüft.

## Ergebnisse

Geprüfter Kandidat: `f9d204ae71a6bdc0643841c6433d145a5b792ab1`.
Frischer Klon mit `git clone --no-local` aus dem versionierten Repository nach
`tmp/bewertung-2-final`, keine Übernahme unversionierter Arbeitsdateien.
`.env` wurde ausschließlich aus der dortigen `.env.example` kopiert; die
Datei-Hashes waren identisch. Build und Tests liefen aus diesem Klon.
Compose-Projekt `live-final` mit eigenen neuen Volumes; S2–S8 danach auf genau
diesem Stack ohne Queue-Purge, Tabellen-Reset oder Volume-Löschung.

| Szenario | Ergebnis am 13.11.2026 |
|---|---|
| S1 | BUILD SUCCESS um 10:23:01 Uhr, 2:56 min, 45 Tests (39 chat-service, 6 batch-writer), 0 Fehler/Fehlschläge/übersprungene Tests. |
| S2 | Frischer Build erfolgreich; alle vier Dienste laufen, 0 veröffentlichte Ports. |
| S3 | Beide A-Clients je 400 vollständige Nachrichten genau einmal und in Absenderfolge; B: 0. |
| S4 | 2 Live-Queues mit je 1 Consumer werden nach 4,734 s zu 1 Queue mit 1 Consumer; chat.persist und chat.dlq bleiben. |
| S5 | 100 live, letzte Zustellung 0,000615 s nach letztem POST; höchste Einzellatenz 0,009661 s. Nach Neustart 100 IDs in SQL, Wartezeit 3,906 s. |
| S6 | 5000 live, Sendedauer 6,771 s; 102 Stichproben, maximal 999 Ready + 1 Unacked = 1000; größter Abfrageabstand 0,106715 s. Nach Fortsetzen 3 neue Nachrichten je Client. |
| S7 | Nach 10 s je 50 fehlende Nachrichten auf beiden Instanzen, anschließend je 1 neue live; keine Duplikate. Speicherweg aus, danach alle 52 IDs in SQL nach 3,844 s. |
| S8 | 25 Java-Dateien, 157 heuristisch erfasste Deklarationen mit Kommentaren; keine Java-Streams, .env nicht versioniert. Zusätzlich manuelle Durchsicht, einschließlich kompakter Record-Konstruktoren. |

Versionierte kompakte Messwerte: [live-abnahme-messwerte.json](live-abnahme-messwerte.json).
Der vollständige lokale Bericht einschließlich IDs, Queue-Stichproben und Latenzen
liegt in `tmp/bewertung-2-final/tmp/live-all-result.json`.
Maven-/Build-/Ablauflogs: `tmp/step7-clone-maven.log`, `tmp/step7-clone-build.log`,
`tmp/step7-clone-all.log`. Diese lokalen Rohdateien sind bewusst nicht versioniert;
alle Prüfungen können mit dem versionierten Skript wiederholt werden.

Die Ergebnisse werden wie im Plan vorgesehen nach dem Kandidaten als separater
Dokumentations-Commit festgehalten. An Produktionscode und Tests wurde nach diesem
erfolgreichen Lauf nichts geändert. Dies ist die eigene reproduzierte Abnahme,
keine vorweggenommene Bewertung durch die Lehrperson.

## Code-Review

Die Quelltextprüfung umfasst Klassen-/Methodenkommentare, verständliche deutsche
Erklärungen, Schleifen statt Java-Streams sowie Netzwerkschreiben außerhalb der
gemeinsamen Empfangssperre. Die Prüfung ersetzt nicht das Gespräch mit der Lehrperson.
Die Orientierung zum eigenen Code steht in `code-review-live-zustellung.md`.
