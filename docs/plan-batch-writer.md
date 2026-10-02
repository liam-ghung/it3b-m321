# Umsetzungsplan: Batch-Writer

Erstellt am 02.10.2026 nach der Spezifikation und vor dem Programmcode.
Basis: [Spezifikation](spec-batch-writer.md). Keine nachträglich erfundene Historie.
Die Aufgaben werden in dieser Reihenfolge umgesetzt und einzeln committed.

| Nr. | Aufgabe und Reihenfolge | Prüfung vor Commit | Geplanter Commit |
|---|---|---|---|
| 1 | Spezifikation schreiben: Vertrag, Fehlerfälle, Messung zuerst festlegen. | S1–S8 gegen den PDF-Auftrag und vorhandenen Code abgleichen. | `docs: Batch-Writer-Vertrag und Abnahme vor Umsetzung spezifizieren` |
| 2 | Diesen Plan festhalten, damit die Bau-Reihenfolge vor dem Code sichtbar ist. | Jeder Schritt hat einen Test und ein Thema. | `docs: getestete Bau-Schritte für Batch-Writer planen` |
| 3 | Eltern-POM, Nachrichtenvertrag und Broker-Topologie erweitern. Diese bilden die Schnittstellen für den Writer. | Root-Maven-Tests; POST auf beiden Pfaden; Validierung und JSON-Felder. | `feat: Bewertungsvertrag und Maven-Elternprojekt vorbereiten` |
| 4 | Writer als Maven-Modul bauen: SQL-Batch, explizite Transaktion, manuelles ACK, Fehlerbehandlung. Erst jetzt existiert der dauerhafte Schreibweg. | Root-Maven-Tests mit echten Testcontainers; Duplikate, unvollständiger Batch, DB-Ausfall und Gift-Nachricht. | `feat: Nachrichten transaktional bündeln und nach Commit bestätigen` |
| 5 | Container-Betrieb ohne offene Ports bereitstellen: Dockerfiles, Compose, Schema, `.env.example`. | Compose validieren, Images bauen, frischen separaten Stack starten und Dienstzustand prüfen. | `feat: Batch-Writer im internen Compose-Stack betreiben` |
| 6 | Wiederholbare Abnahme für alle Szenarien und README-Stand ergänzen. Erst der vollständige Stack kann Last, Rückstau und Skalierung zeigen. | `mvn clean test`, S2–S8 nacheinander, Messwerte dokumentieren, Kommentare prüfen. | `test: Bewertungsszenarien und Ergebnisse nachvollziehbar belegen` |

Tests, die einen Fehler finden, werden im zugehörigen Schritt korrigiert und erneut
ausgeführt. Zusätzliche Reparatur-Commits werden mit Grund im Ergebnisbericht
erwähnt. Bestehende fremde Arbeitsdateien werden weder committed noch entfernt.

## Abgabe

Nach erfolgreichen Prüfungen die Commits nach `origin/main` übertragen und den
Tag `bewertung-1` auf den geprüften Abschluss setzen und pushen. Existiert der Tag
bereits, nicht stillschweigend ersetzen. Den Fork-Link im Schulportal muss der
Lernende selbst abgeben. Die Vorbereitung auf das Code-Review folgt am eigenen Code.
