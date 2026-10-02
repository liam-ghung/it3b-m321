# Vorbereitung auf das Code-Review

Keine Antworten auswendig behaupten: Die Stellen im Editor öffnen und in eigenen
Worten erklären. Das Gespräch selbst kann nicht durch eine Datei ersetzt werden.

## Der Ablauf in einem Satz

Der chat-service schickt JSON an RabbitMQ; der batch-writer sammelt Nachrichten,
schreibt einen Stapel in einer Transaktion und bestätigt erst danach den Empfang.

## Vier wichtige Stellen

| Frage | Stelle zum Zeigen | Einfache Erklärung und verhinderter Fehler |
|---|---|---|
| Warum ACK erst nach COMMIT? | `BatchListener.persist`, `repository.saveBatch` vor `basicAck`; `MessageRepository.saveBatch` | Ein ACK entfernt die Lieferung beim Broker. Würden wir vorher bestätigen und dann abstürzen, wäre die Nachricht verloren. |
| Was passiert bei doppelter Zustellung? | `MessageRepository.INSERT`, `ON CONFLICT (id) DO NOTHING` | Die gleiche UUID wird nicht nochmals eingefügt. Wichtig beim Absturz zwischen COMMIT und ACK. |
| Warum ist das wirklich ein Batch? | `BatchConfiguration.batchContainer`, `setBatchSize`, `setPrefetchCount`, Zeitlimits; `MessageRepository.saveBatch` | Bis zu 100 Zeilen teilen eine Transaktion. Das spart Commits. Der Timer verhindert, dass eine einzelne Nachricht liegen bleibt. |
| Was passiert beim DB-Ausfall? | `BatchListener.retryLater`, Pause und `basicNack(..., true)` | Kein erfolgreicher Commit, also kein ACK. Nach einer Pause gibt der Broker die Nachrichten erneut ab. Der Dienst muss nicht von Hand neu gestartet werden. |

Alle genannten Java-Dateien liegen unter
`batch-writer/src/main/java/ch/benedict/m321/batch/`.

## Weitere einfache Lehrerfragen

**Warum reicht `content_type: application/json`?**
`decode` liest den Body direkt mit dem ObjectMapper in `StoredMessage`. Der Writer
braucht keine Klasse des chat-service und keinen `__TypeId__`-Header.

**Was ist Competing Consumers?**
Zwei Writer hören auf dieselbe Queue. RabbitMQ verteilt Lieferungen zwischen ihnen.
Jeder hat seinen eigenen Kanal; bestätigt wird jeweils auf dem Kanal der Lieferung.
Bei Fehlern kann dieselbe Nachricht später erneut zugestellt werden.

**Warum steht bei `basicAck(tag, false)` ein `false`?**
Es wird genau dieser Delivery-Tag bestätigt. Eine Sammelbestätigung könnte auch
andere, noch nicht erfolgreich verarbeitete Lieferungen bestätigen.

**Was macht die DLQ?**
Sie bewahrt fehlerhafte Nachrichten auf. Ungültiges JSON wird einzeln abgelehnt.
Ein Datenfehler im SQL löst einen Rollback aus; danach werden die Zeilen einzeln
versucht, damit gesunde Nachbarn nicht mit in der Fehlerablage landen.

**Warum keine DLQ nach drei Verbindungsfehlern?**
Die Datenbank kann länger ausfallen. Gültige Nachrichten sollen dann warten und
später normal gespeichert werden. Daher kein Zustelllimit für Infrastrukturfehler.

**Ist das Exactly-once?**
Nein. RabbitMQ kann eine Nachricht mehrfach liefern. Das ist At-least-once.
Der Datenbankeffekt bleibt durch den Primärschlüssel wiederholbar ohne Dublette.

**Was zeigt der Ausfalltest?**
Der Maven-Test friert den echten PostgreSQL-Prozess 15 Sekunden ein. Das
Abnahmeskript stoppt und startet zusätzlich den gesamten PostgreSQL-Container im
Compose-Netz. Beide müssen ohne Writer-Neustart erfolgreich sein.

**Warum werden keine Räume angelegt?**
Die Bewertung verlangt den Schreibweg, nicht Raumverwaltung. Daher wird jede
gültige Raum-UUID gespeichert und hier kein Raum-Fremdschlüssel erzwungen.

**Warum keine Ports auf localhost?**
Die Bewertung prüft intern mit `docker compose exec`. In Compose ist kein
`ports`-Eintrag vorhanden. Die Dienste erreichen einander über ihre Dienstnamen.

## Vorschläge, die während der Umsetzung korrigiert wurden

- Die ältere IT3b-Struktur passte nicht direkt zu `/messages` und dem geforderten
  Datenmodell. Der bestehende Fork wurde angepasst; ein anderer Fork wurde nicht
  als angeblich eigener Ausgangsstand übernommen.
- Ein erster Integrationstest stoppte einen Testcontainer mit zufälligem Host-Port.
  Nach dem Start war der vorherige Port nicht mehr erreichbar. Der Test wurde auf
  echtes Pausieren umgestellt; vollständiges Stop/Start wird separat im Compose-Netz
  mit stabilen Dienstnamen geprüft.
- Ein grüner Test mit Mocks beweist keine Datenbank- oder Queue-Verbindung. Deshalb
  laufen die Writer-Tests gegen echte Testcontainers und die Abnahme zusätzlich
  gegen den gebauten Compose-Stack.
