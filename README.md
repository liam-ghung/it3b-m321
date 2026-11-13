# M321 — Chat-App (Klasse IT3b)

Lernprojekt zum Modul **M321 Verteilte Systeme / Microservices**. Wir bauen gemeinsam eine
Chat-Anwendung aus mehreren Services, die über eine Message Queue miteinander reden und mit
docker-compose gestartet werden.

## Für Lernende: so startest du

1. Dieses Repository **forken** (Button «Fork» oben rechts).
2. Deinen Fork klonen:
   ```bash
   git clone https://github.com/<dein-benutzername>/it3b-m321.git
   cd it3b-m321
   ```
3. Voraussetzungen installieren: **Java 21**, **Maven**, **Docker Desktop**, **Git**.
4. Die Planung lesen (siehe unten) — erst verstehen, dann programmieren.

Alle Aufgaben werden in **deinem Fork** gelöst. Das Original-Repository bleibt die Referenz.

## Was gebaut wird

| Baustein | Technologie | Aufgabe |
|---|---|---|
| chat-service | Spring Boot 3, Java 21 | REST-API, liefert Nachrichten live per SSE, prüft das Login-Token |
| batch-writer | Spring Boot 3, Java 21 | Schreibt Nachrichten gebündelt und wiederholbar in PostgreSQL |
| gateway | nginx | Einziger nach aussen offener Port, Reverse Proxy |
| keycloak | Keycloak | Login (OIDC) |
| rabbitmq | RabbitMQ | Message Queue zwischen den Services |
| postgres | PostgreSQL | Speichert den Chat-Verlauf |
| Web-UI | React | Browser-Client |
| Desktop-UI | JavaFX | Zweiter Client gegen dieselbe API |

Der aktuelle Bewertungsstack läuft vollständig im Docker-Netz `chat-net` und veröffentlicht
keinen Port. Das Gateway und die Clients gehören zum späteren Ausbau.

## Dokumente

- [`docs/spec-batch-writer.md`](docs/spec-batch-writer.md) — Vertrag, Datenmodell,
  Fehlerverhalten und messbare Anforderungen der Bewertung 1.
- [`docs/spec-live-zustellung.md`](docs/spec-live-zustellung.md) — Vertrag und Grenzen für Bewertung 2.
- [`docs/plan-live-zustellung.md`](docs/plan-live-zustellung.md) — vorab gepushter Plan mit Prüfnotizen.
- [`docs/test-live-zustellung.md`](docs/test-live-zustellung.md) — Gesamtabnahme aus frischem Klon.
- [`docs/code-review-live-zustellung.md`](docs/code-review-live-zustellung.md) — einfache Code-Erklärung und Lehrerfragen.
- [`docs/plan-batch-writer.md`](docs/plan-batch-writer.md) — vorab versionierte Bau-Schritte mit Tests.
- [`docs/test-batch-writer.md`](docs/test-batch-writer.md) — gemessene Ergebnisse der lokalen Abnahme.
- [`docs/code-review-batch-writer.md`](docs/code-review-batch-writer.md) — einfache Erklärungen und Lehrerfragen zum Code.
- [`docs/UMSETZUNG.md`](docs/UMSETZUNG.md) — umgesetzte Anforderungen, Erklärungen,
  Testergebnisse und noch offene Live-Prüfungen.
- [`PLANUNG.md`](PLANUNG.md) — Stack, Architektur, Nachrichtenfluss, Datenmodell, offene Punkte.
  Das ist die Grundlage für alles Weitere.
- [`docs/design/2026-08-28-chat-app-architektur.html`](docs/design/2026-08-28-chat-app-architektur.html)
  — grafische Fassung der Architekturdiagramme, lokal im Browser öffnen (funktioniert ohne Internet).
- [`docs/plan/2026-09-04-chat-service-bootstrap.md`](docs/plan/2026-09-04-chat-service-bootstrap.md)
  — Schritt-für-Schritt-Plan für den ersten Service: Projekt anlegen, Datenbank und Broker
  anbinden, Nachrichten lesen und senden. Jeder Schritt mit Test.
- [`CLAUDE.md`](CLAUDE.md) — Codestil-Regeln für dieses Projekt. Gelten auch für dich.
- `docs/skizze-architektur.heic` — die Handskizze aus dem Unterricht, von der die Planung ausgeht.

## Codestil, kurz

Der Massstab ist: **kann eine lernende Person jede Zeile vorlesen und sagen, was sie tut?**

- Eine Anweisung pro Zeile, Zwischenresultate in benannte Variablen.
- `for`-Schleife statt Stream, `if` statt verschachteltem Ternary.
- Sprechende Namen in ganzen Wörtern.
- Über jeder Methode ein bis zwei Sätze: was sie tut und warum es sie gibt.
- Kommentare auf Deutsch, als Erklärung an eine Mitlernende.

Die vollständigen Regeln stehen in [`CLAUDE.md`](CLAUDE.md).

## Stand

| Bereich | Stand |
|---|---|
| chat-service | POST `/messages`, alter Pfad `/api/messages` bleibt als Alias; Server-UUID und Sendezeit |
| Nachrichtenvertrag | `id`, `roomId`, `senderId`, `senderName`, `content`, `sentAt` |
| RabbitMQ | Dauerhafte Queue `chat.persist`, Bindung an `chat.messages`, Fehlerablage `chat.dlq` |
| Live-Empfang (Bewertung 2) | `chat.messages` verteilt zusätzlich an `chat.delivery`; eigene flüchtige Queue je Instanz, begrenzter lokaler Empfangspuffer |
| Live-Anzeige | GET `/stream?roomId=…` liefert SSE nur für den gewählten Raum; ID und JSON, Heartbeats, begrenzte Client-Puffer |
| Neuverbindung (Schritt 6) | `Last-Event-ID` liefert spätere Nachrichten desselben Raums aus dem lokalen 10000er-/120-s-Puffer und geht geordnet zu live über; ungültige UUID: 400, nicht verfügbarer oder raumfremder Cursor: 409 |
| batch-writer | 100 Nachrichten oder 200 ms, eine DB-Transaktion pro Stapel, ACK nach COMMIT |
| Duplikate | `ON CONFLICT (id) DO NOTHING` verhindert doppelte Zeilen |
| Ausfälle | Automatische Wiederholung bei DB-Ausfall, ungültige Nachrichten einzeln in DLQ |
| Skalierung | Mehrere Writer als Competing Consumers an derselben Queue |
| Tests | 45 Maven-Tests mit echtem RabbitMQ/PostgreSQL; Bewertung 2: `scripts/acceptance-live.py all` prüft S2–S8 in Reihenfolge |
| Später | Login, Raumverwaltung, Gateway und Benutzeroberflächen |

Die älteren Dokumente `PLANUNG.md` und `docs/UMSETZUNG.md` beschreiben den ursprünglichen
Bootstrap. Insbesondere Ports, Queue-Namen und die dort noch fehlende Speicherung sind
durch die Batch-Writer-Spezifikation für Bewertung 1 überholt.

## Lokal starten

Voraussetzungen: Java 21, Maven und laufende Docker Engine. Im Projektverzeichnis
einmal `.env.example` nach `.env` kopieren (Windows: `copy .env.example .env`,
PowerShell: `Copy-Item .env.example .env`, Linux/macOS: `cp .env.example .env`).
Vorhandene `.env` nicht überschreiben. Danach:

```bash
mvn clean test
docker compose up -d --build
docker compose ps
```

`mvn clean test` startet eigene kurzlebige PostgreSQL- und RabbitMQ-Testcontainer mit
zufälligen Testports. Der Compose-Stack selbst veröffentlicht keine Ports. Es werden
keine Tests übersprungen, wenn Docker fehlt; dann schlägt der Lauf sichtbar fehl.

Health lässt sich innerhalb des Netzes prüfen:

```bash
docker compose exec chat-service curl -fsS http://localhost:8080/actuator/health
```

Die API-Dokumentation liegt intern unter `/v3/api-docs` bzw. `/swagger-ui.html`.
`POST /messages` erwartet beispielsweise:

```json
{
  "roomId": "11111111-1111-1111-1111-111111111111",
  "senderId": "lernende1",
  "senderName": "Lernende 1",
  "content": "Hallo zusammen"
}
```

Die Antwort ist `202 Accepted`; UUID und Sendezeit vergibt der Server. Der Writer
speichert asynchron kurz danach. Die alten Eingabefelder `sender` und `text` sind
weiterhin erlaubt. Der alte Absender wird dann als ID und Anzeigename verwendet.

## Bewertungsszenarien ausführen

Mit Python 3 (keine zusätzlichen Python-Pakete nötig):

```bash
python scripts/acceptance.py all
```

Das Skript führt S2–S8 ohne Löschen der Daten zwischen Szenarien aus. Es sendet 3300
HTTP-Nachrichten plus das direkt zugestellte Duplikat, skaliert auf zwei Writer und
stoppt PostgreSQL für 15 Sekunden. Nur auf dem lokalen Schulungsstack ausführen.
Zum Einzeltest zum Beispiel `python scripts/acceptance.py S5`. Ein abweichender
Compose-Projektname wird mit `--project NAME` angegeben. Messwerte stehen nach
Erfolg in `tmp/acceptance-result.json`. S1 ist der separate Root-Maven-Lauf.

```bash
docker compose logs --tail 40 batch-writer
docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM message"'
```

S8 prüft automatisch Streams, vorhandene Kommentare und die unversionierte `.env`.
Die Verständlichkeit der Kommentare wird zusätzlich manuell geprüft.

Die SQL-Dateien unter `db/` werden nur beim ersten Start mit leerem Datenbankvolume
ausgeführt. `docker compose stop` stoppt die Infrastruktur und erhält die Daten.
Die neuen Volumes `postgres-batch-data` und `rabbitmq-batch-data` vermeiden eine
Verwechslung mit der alten Bootstrap-Datenbank. Alte Volumes werden nicht gelöscht.
Die Tabellen des neuen Vertrags heissen `message(id, room_id, sender_id, sender_name,
content, sent_at)`. Räume werden beim Schreiben bewusst nicht geprüft; Raumverwaltung
gehört nicht zur Bewertung.

## Live-Zustellung: Gesamtabnahme für Bewertung 2

Nach frischem Klon `.env.example` als `.env` kopieren. Mit Java 21, Maven,
laufendem Docker und Python 3 (ohne Zusatzpakete):

```bash
mvn clean test
docker compose up -d --build
python scripts/acceptance-live.py all
```

S3 sendet 400 Nachrichten. S5 stoppt Writer und Datenbank, sendet 100 Nachrichten
und startet beide Dienste auch bei Fehlern wieder. Der Testclient läuft kurzzeitig
als `python:3.13-alpine` im Docker-Netz, ohne Host-Ports. Es werden keine bestehenden
Daten gelöscht. Zwei Chat-Instanzen bleiben danach aktiv.
Messwerte stehen in `tmp/live-S3-result.json` und `tmp/live-S5-result.json`.
S6 pausiert eine Chat-Instanz, sendet 5000 Nachrichten über die gesunde Instanz und
misst Ready plus Unacked. Die Pause wird auch bei Fehlern aufgehoben; anschließend
werden neue Nachrichten an beiden Instanzen geprüft. Die lokale Puffergrenze wird
zusätzlich im Maven-Lauf getestet. Ergebnisse: `tmp/live-S6-result.json` und
[Schritt 5](docs/live-zustellung-schritt-5.md).
`all` führt S2–S8 in Auftragsreihenfolge auf demselben Stack aus, ohne Daten zu
löschen. Der Bericht steht unter `tmp/live-all-result.json`. Einzelne Szenarien
sind ebenfalls möglich, zum Beispiel `python scripts/acceptance-live.py S7`.
S2 kontrolliert die Dienste und Ports; den frischen Klon und Build führt man davor
aus. S4 misst das Entfernen einer Live-Queue nach Scale-down. S8 prüft Quelltext
und unversionierte `.env`; Kommentare werden zusätzlich manuell gelesen.
S7 prüft die Nachlieferung von 50 Nachrichten nach zehn Sekunden auf beiden
Instanzen und danach neue Live-Nachrichten. Writer und Datenbank sind dabei
gestoppt und werden anschließend wieder gestartet; alle 52 Testnachrichten
(Anker, 50 verpasste, eine neue) werden danach in SQL nachgewiesen.
Ergebnisse: `tmp/live-S7-result.json`, Erklärung: [Schritt 6](docs/live-zustellung-schritt-6.md).
Details und Ergebnisse: [Schritt 4](docs/live-zustellung-schritt-4.md).

## Abgabe

Für Bewertung 2 nach erfolgreicher Gesamtabnahme den geprüften Stand auf `main`
hochladen und den Tag `bewertung-2` setzen und pushen. Bewertet wird dieser Tag.
`bewertung-1` bleibt unverändert als frühere Abgabe erhalten. Den
[Fork-Link](https://github.com/liam-ghung/it3b-m321) im Schulportal einreichen.
