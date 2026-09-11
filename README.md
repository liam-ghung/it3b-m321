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
| batch-service | Spring Boot 3, Java 21 | Einziger Schreiber in die Datenbank, speichert Nachrichten gebündelt |
| gateway | nginx | Einziger nach aussen offener Port, Reverse Proxy |
| keycloak | Keycloak | Login (OIDC) |
| rabbitmq | RabbitMQ | Message Queue zwischen den Services |
| postgres | PostgreSQL | Speichert den Chat-Verlauf |
| Web-UI | React | Browser-Client |
| Desktop-UI | JavaFX | Zweiter Client gegen dieselbe API |

Alles unterhalb des Gateways läuft in einem internen Docker-Netzwerk und ist von aussen nicht
erreichbar.

## Dokumente

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

Der Bootstrap ist implementiert: `chat-service` liest den Verlauf aus PostgreSQL und
publiziert neue Nachrichten auf den RabbitMQ-Fanout-Exchange `chat.messages`.

## Lokal starten

Docker Desktop starten, danach im Projektverzeichnis:

```bash
docker compose up -d
cd chat-service
mvn test
mvn spring-boot:run
```

- [Swagger UI](http://localhost:8080/swagger-ui.html): GET und POST ausprobieren.
- [Health](http://localhost:8080/actuator/health): Zustand von Datenbank und Broker.
- [RabbitMQ](http://localhost:15672): Benutzer `chat`, Passwort `chat`.
- Demo-Raum: `11111111-1111-1111-1111-111111111111`, mit drei Nachrichten.

`GET /api/messages?roomId=11111111-1111-1111-1111-111111111111` liefert den Verlauf,
neueste Nachricht zuerst. `limit` ist optional (Standard 50, erlaubt 1 bis 100).
`POST /api/messages` erwartet beispielsweise:

```json
{
  "roomId": "11111111-1111-1111-1111-111111111111",
  "sender": "lernende1",
  "text": "Hallo zusammen"
}
```

Die Antwort ist `202 Accepted`; UUID und Sendezeit vergibt der Server.
Der Absender ist bis zur Keycloak-Erweiterung ein Platzhalter. Der Bootstrap enthält
noch keinen Login, keine Live-Anzeige und keinen schreibenden Batch-Service.
Gesendete Nachrichten erscheinen deshalb noch nicht im Datenbankverlauf.

Für den Broker-Test zuerst in RabbitMQ eine Queue `test.listen` anlegen und an
`chat.messages` binden. Ohne gebundene Queue verwirft der Exchange Nachrichten.
Nach einem POST lässt sich das JSON über «Get messages» in der Queue kontrollieren.
Die Test-Queue anschliessend löschen.

Die SQL-Dateien unter `db/` werden nur beim ersten Start mit leerem Datenbankvolume
ausgeführt. `docker compose stop` stoppt die Infrastruktur und erhält die Daten.
Die veröffentlichten Datenbank- und Broker-Ports gehören zur lokalen Bootstrap-Phase.
