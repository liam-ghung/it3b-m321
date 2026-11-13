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

Der Kandidat wird zuerst committed und anschließend aus einem frischen Klon
geprüft. Die tatsächlichen Ergebnisse werden danach in einem gesonderten
Dokumentations-Commit ergänzt. Bis dahin wird keine bestandene Gesamtabnahme behauptet.

## Code-Review

Die Quelltextprüfung umfasst Klassen-/Methodenkommentare, verständliche deutsche
Erklärungen, Schleifen statt Java-Streams sowie Netzwerkschreiben außerhalb der
gemeinsamen Empfangssperre. Die Prüfung ersetzt nicht das Gespräch mit der Lehrperson.
Die Orientierung zum eigenen Code steht in `code-review-live-zustellung.md`.
