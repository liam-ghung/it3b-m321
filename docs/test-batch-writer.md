# Lokale Abnahme am 02.10.2026

Geprüft mit Java 21, Maven und Docker Desktop. Dies sind eigene wiederholbare
Prüfungen anhand des Auftrags, nicht das unbekannte Prüfungsskript des Lehrers.

## Ergebnisse

| Szenario | Tatsächliches Ergebnis |
|---|---|
| S1 | `mvn -B -ntp clean test`: BUILD SUCCESS; 22 Tests, 0 Fehler, 0 fehlgeschlagen, 0 übersprungen (16 Chat-Service, 6 Writer). |
| S2 | Vier Compose-Dienste laufen, Health UP, 0 veröffentlichte Host-Ports. |
| S3 | 1000 über HTTP gesendete Nachrichten vollständig gespeichert; nach dem Senden nach 2,39 s geleert. |
| S4 | 1000 Nachrichten bei gestopptem Writer aufgestaut; nach Start vollständig gespeichert, 17 Transaktionen inklusive Messabfragen, 7,34 s bis leer. Grenze: 100 Transaktionen. |
| S5 | Dasselbe rohe JSON zweimal direkt an `chat.persist`: genau eine Zeile, kein zusätzlicher DLQ-Eintrag. Nur JSON-Content-Type, kein Java-Typheader. |
| S6 | Zwei Consumer nach Skalierung; weitere 1000 Nachrichten vollständig gespeichert, nach dem Senden nach 2,42 s geleert. |
| S7 | PostgreSQL 15 s gestoppt, 300 Nachrichten während des Ausfalls gesendet; alle nach insgesamt 22,03 s gespeichert, 0 Writer-Neustarts. Grenze: 90 s. |
| S8 | Sechs Writer-Java-Dateien automatisch geprüft und Kommentare manuell gelesen; keine Streams, deutsche Klassen-/Methodenkommentare, `.env` nicht versioniert. |

S2–S8 liefen nacheinander ohne Zurücksetzen der Datenbank oder Queue:

```bash
python scripts/acceptance.py all --project m321-bewertung
```

Das Skript prüft die tatsächlich zurückgegebenen IDs je Testraum, nicht bloss die
Gesamtzahl aller vorhandenen Zeilen. Die Zeiten S3/S6 messen das Leeren nach dem
letzten POST; die Prüfung wartet höchstens 60 Sekunden. S7 misst ab dem Ausfall.
S4 zählt auch die Messabfragen mit und liegt trotzdem deutlich unter der Grenze.

Die sechs Writer-Integrationstests verwenden echte PostgreSQL-/RabbitMQ-Container:
Duplikate, Teilstapel, 1000er-Rückstau, ungültiges JSON, SQL-Datenfehler mit gesundem
Nachbarn und 15 Sekunden eingefrorene Datenbank mit automatischer Erholung.
Der komplette Container-Neustart wird zusätzlich in S7 geprüft.

## Zusätzlicher Start aus frischem Klon

Nach dem Test-Commit `daf6522` wurde mit `git clone --no-hardlinks` ein neuer lokaler
Klon erstellt. Er enthält nur versionierte Dateien. Darin wurde `.env` neu aus
`.env.example` erstellt und `docker compose -p m321-fresh up -d --build` ausgeführt.
Beide Images wurden erfolgreich gebaut; Docker legte neue, leere Datenvolumes an.
`python scripts/acceptance.py S2 --project m321-fresh` bestand mit allen vier
Diensten, Health UP und null veröffentlichten Ports. S3–S8 wurden bereits am
identischen Programmcode im obigen Lauf geprüft. Dieser zusätzliche Dokumentations-
Commit hält die erst nach dem Klonen vorliegende Bestätigung fest.

## Grenzen und Abgabe

Die mündliche Erklärung bleibt Teil der Bewertung. Eine Vorbereitung steht in
[code-review-batch-writer.md](code-review-batch-writer.md). Die Spezifikation wurde
nicht als vom Lehrer freigegeben ausgegeben. Den Fork-Link im Schulportal muss
der Lernende selbst einreichen.
