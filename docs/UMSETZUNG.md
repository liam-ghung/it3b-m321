# Umsetzung des Chat-Service-Bootstraps

Grundlage: [Bootstrap-Plan vom 04.09.2026](plan/2026-09-04-chat-service-bootstrap.md)
und [Codestil-Regeln](../CLAUDE.md).

## 1. Projekt und API-Dokumentation

Der Dienst verwendet Java 21, Spring Boot 3.5.16 und Maven. Swagger UI wird mit
springdoc-openapi 2.9.0 bereitgestellt. Die OpenAPI-Konfiguration beschreibt Titel,
Version und Zweck der API. Über `/actuator/health` kann der Zustand geprüft werden.

Die Anwendung ist in Controller, Service und Repository aufgeteilt. Der Controller
bearbeitet HTTP-Anfragen, der Service enthält die Fachlogik und das Repository das SQL.
Diese Trennung ist Prüfungsstoff: Jede Schicht hat eine klar abgegrenzte Aufgabe.

## 2. Datenbank und Broker

`docker-compose.yml` definiert PostgreSQL 17 und RabbitMQ 4 mit Management-Oberfläche.
Beide gehören zum Netzwerk `chat-net`. Das PostgreSQL-Volume erhält die Daten über
Container-Neustarts hinweg. Die Ports sind in dieser Bootstrap-Phase veröffentlicht,
weil der Chat-Service auf dem Host läuft. Das spätere Gateway ist noch nicht umgesetzt.

`db/01-schema.sql` erstellt `room`, `room_member` und `message` mit Primär- und
Fremdschlüsseln. Ein Index auf Raum und Sendezeit unterstützt die Verlaufsabfrage.
`db/02-demo-data.sql` fügt einen Raum, Mitglieder und drei Beispielnachrichten ein.
Zeitpunkte werden als `TIMESTAMPTZ` gespeichert.

Prüfungsstoff sind Container-Netzwerke, veröffentlichte Ports, dauerhafte Volumes
und der Zusammenhang zwischen Tabellen, Schlüsseln und Abfragen.

## 3. Nachrichtenverlauf lesen

`GET /api/messages?roomId=…&limit=…` liest die neuesten Nachrichten eines Raums.
Der Standardwert für `limit` ist 50; erlaubt sind 1 bis 100. Ungültige Eingaben
werden mit HTTP 400 abgelehnt. Erfolgreiche Abfragen liefern HTTP 200 und JSON.

Das Repository verwendet `JdbcTemplate` mit SQL-Platzhaltern. Eingaben werden als
Parameter übergeben und nicht in den SQL-Text eingebaut. Die Ergebnisse werden
explizit in `Message`-Datensätze übertragen. Prüfungsstoff: Prepared Statements,
SQL-Injection vermeiden, Sortierung und Begrenzung der Ergebnismenge.

## 4. Nachrichten senden und Fanout-Aufgabe

`POST /api/messages` nimmt `roomId`, `sender` und `text` entgegen. Raum-ID, Absender
und Text werden vor der Weitergabe geprüft. UUID und Sendezeit erzeugt der Service.
Anschliessend publiziert er das Objekt als JSON auf `chat.messages`.

Die Übungsaufgabe in `chatExchange()` ist gelöst: Der Fanout-Exchange heisst
`chat.messages`, ist dauerhaft und wird nicht automatisch gelöscht. Der
JSON-Konverter verwendet den von Spring konfigurierten ObjectMapper.

Ein Fanout-Exchange verteilt jede Nachricht an alle gebundenen Queues und ignoriert
den Routing-Key. Ohne gebundene Queue wird die Nachricht verworfen. Der Exchange
selbst speichert keine Nachrichten.

Der Chat-Service schreibt beim Senden nicht in die Datenbank. Deshalb lautet die
Antwort HTTP 202 Accepted: Die Nachricht wurde weitergegeben, aber noch nicht
gespeichert. Der spätere Batch-Service übernimmt das Speichern. Die serverseitige
UUID ermöglicht dabei später die Erkennung erneut zugestellter Nachrichten.
Diese Entkopplung sowie der Unterschied zwischen HTTP 202 und 201 sind Prüfungsstoff.

## 5. Codestil

Klassen, Methoden, Variablen und API-Felder verwenden englische Namen. Kommentare,
Log-Ausgaben und API-Beschreibungen sind auf Deutsch. Abhängigkeiten werden über
Konstruktoren übergeben. Es werden weder Lombok noch JPA eingesetzt; SQL und
Datensatz-Zuordnung bleiben sichtbar.

## 6. Nachweis und Live-Prüfungen

Ausgeführt am 11.09.2026 mit Java 21:

```bash
mvn -B -ntp -f chat-service/pom.xml test
```

Ergebnis: **BUILD SUCCESS, 15 Tests, 0 Fehler, 0 fehlgeschlagene Tests.**

| Bereich | Nachweis |
|---|---|
| Anwendungskontext | 1 Test: Spring kann die Anwendung zusammenbauen |
| HTTP-Schnittstelle | 13 Testfälle: Verlauf, HTTP 202, leerer Text, ungültige Limits und Pflichtfelder |
| Fachlogik | 1 Test: UUID und Zeit vom Server, Publizieren am richtigen Exchange, kein Repository-Zugriff beim Senden |

Die Controller- und Service-Tests verwenden Attrappen. Auch der erfolgreiche
Kontextstart beweist keine funktionierende Verbindung zu PostgreSQL oder RabbitMQ.
Die Compose-Konfiguration wurde mit `docker compose config --quiet` geprüft.

Die zunächst wegen der nicht erreichbaren Docker Engine offenen Live-Prüfungen
wurden am 11.09.2026 vom Lernenden nachgeholt. Grundlage der folgenden Ergebnisse
sind seine Rückmeldungen und die im Gespräch gezeigten Screenshots:

| Live-Prüfung | Ergebnis und Nachweis |
|---|---|
| Infrastruktur starten | Screenshot bestätigt PostgreSQL und RabbitMQ als `Started`. |
| Health | Screenshot von `/actuator/health` zeigt `db` und `rabbit` jeweils als `UP`. |
| Verlauf über Swagger | Lernender bestätigt die erfolgreiche GET-Abfrage mit drei Demo-Nachrichten. |
| Nachricht über Swagger senden | Lernender bestätigt den beschriebenen POST-Test; der Broker-Screenshot belegt den Empfang. HTTP 202 ist zusätzlich im automatisierten Controller-Test geprüft. |
| JSON im Broker | Screenshot der Queue `test.listen` zeigt den Exchange `chat.messages` und den Text `Mein RabbitMQ-Test` mit UUID, Raum-ID, Absender und Sendezeit. |
| Senden und Speichern getrennt | Lernender bestätigt nach dem Senden weiterhin drei Nachrichten im Verlauf; die Testnachricht wurde nicht in die Datenbank geschrieben. |
| Aufräumen | Lernender bestätigt, dass die Test-Queue `test.listen` anschliessend gelöscht wurde. |

Damit ist der beschriebene Live-Funktionstest abgeschlossen. Die Anleitung zum
Wiederholen steht in der [README](../README.md). Die Screenshots wurden im Gespräch
gezeigt und sind nicht als Dateien im Repository abgelegt.

## 7. Abgrenzung und Versionsgeschichte

Keycloak, Raumverwaltung, SSE, Batch-Service, Gateway, React und JavaFX sind laut
Bootstrap-Plan spätere Aufgaben. `sender` ist bis zur Token-Prüfung ein Platzhalter.

Der Plan fordert getrennte Commits für Dokumente und die vier Bau-Schritte.
Die Ausgangsdokumente sind bereits in der übernommenen Repository-Historie enthalten.
Die Implementierung wurde zunächst gemeinsam committed. Am 11.09.2026 wurde dieser
Commit nachträglich in die vier Bau-Schritte des Plans aufgeteilt. Die Aufteilung
rekonstruiert prüfbare Zwischenstände; sie behauptet keinen ursprünglich so
durchgeführten Entwicklungsablauf. Der ursprüngliche Commit `5628960` ist lokal
über den Sicherungsbranch `codex/backup-bootstrap-5628960` erhalten.

| Schritt | Commit-Titel | Prüfung des Zwischenstands |
|---|---|---|
| Task 1 | `feat(chat-service): Projekt aufsetzen, Swagger UI erreichbar` | Maven: 1 Test bestanden |
| Task 2 | `feat(infra): PostgreSQL und RabbitMQ in docker-compose, Schema und Demo-Daten` | Maven: 1 Test bestanden |
| Task 3 | `feat(chat-service): GET /api/messages liest den Verlauf, dokumentiert in Swagger` | Maven: 2 Tests bestanden |
| Task 4 | `feat(chat-service): POST /api/messages publiziert auf den Fanout-Exchange` | Maven: 15 Tests bestanden; Live-Nachweise siehe Abschnitt 6 |

Jeder dieser Commits enthält die zusammengehörenden Änderungen und eine Beschreibung.
Zusammen mit dem bereits vorhandenen Dokumentations-Commit bilden sie die im Plan
geforderte Folge aus Dokumentation und vier Bau-Schritten.
