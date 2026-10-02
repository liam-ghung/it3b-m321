# Spezifikation: batch-writer (Bewertung 1)

Stand: 02.10.2026, vor Implementierung geschrieben. Grundlage ist der bestehende
IT3b-Fork. Die Bewertungsvorgaben haben für diesen Ausbau Vorrang vor dem alten
Bootstrap. Diese Spezifikation kann der Lehrperson vorgelegt werden; eine Freigabe
der Lehrperson wird hier nicht behauptet.

## Zweck und Abgrenzung

Der Dienst konsumiert `chat.persist` und schreibt Nachrichten gebündelt in PostgreSQL.
Er besitzt weder HTTP-Endpunkte noch Login, Raumverwaltung oder eine Oberfläche.
Mehrere Instanzen teilen eine Queue (Competing Consumers). Keine neuen Funktionen
für Historie, Gateway oder Lastgenerator; das Abnahmeskript ist nur ein Testwerkzeug.

## Abgleich mit dem bestehenden Fork

Der IT3b-Bootstrap hat `/api/messages`, `sender` und `text`, keinen Eltern-POM und
noch keine Persistenz-Queue. Der Bewertungsauftrag nennt `/messages` und das Modell
mit `sender_id`, `sender_name`, `content`. Deshalb wird der vorhandene Service
gezielt erweitert, nicht durch einen anderen Fork ersetzt. `/api/messages` bleibt
als Pfadalias erhalten. Alte Request-Felder `sender` und `text` werden akzeptiert;
der Absendername entspricht dann mangels separatem Namen der Absender-ID.

Belege: vorhandene Klassen `message/Message.java`, `NewMessage.java`,
`MessageService.java` und `rabbit/RabbitConfiguration.java` im Commit `c35d7ae`.
Das neue Bewertungsformat ist zusätzlich mit der öffentlichen Lehrervorlage
`pritzit-mpritz/it3c-m321`, Commit `f8ea557e632ae68a38bbf7f98984891683be19a7`,
abgeglichen: `dto/ChatMessage.java`, `dto/SendMessageRequest.java` und PLANUNG 3.7.
Die fachlichen Festlegungen für diesen IT3b-Fork stehen vollständig hier.

## Nachrichtenvertrag

POST `/messages` erhält `roomId` (UUID), `senderId`, `senderName`, `content`
(je nicht leer). Der Server vergibt `id` und `sentAt`; Antwort 202 mit der Nachricht.
Das auf `chat.messages` veröffentlichte JSON wird über eine dauerhafte Bindung an
`chat.persist` verteilt. Auch direkte Zustellung an `chat.persist` funktioniert.

```json
{
  "id": "aaaaaaaa-0000-0000-0000-000000000001",
  "roomId": "11111111-1111-1111-1111-111111111111",
  "senderId": "lernende1",
  "senderName": "Lernende 1",
  "content": "Hallo",
  "sentAt": "2026-10-02T08:00:00Z"
}
```

Alle sechs Felder sind Pflicht. UUIDs müssen gültig sein, `sentAt` ist ISO-8601 mit
Zeitzone, Absenderfelder sind maximal 255 Zeichen lang, Inhalt ist nicht blank.
Der Writer liest JSON explizit in seine eigene Datenklasse. Er benötigt nur
`content_type: application/json`, keinen Java-Klassennamen und keinen `__TypeId__`.
Unbekannte JSON-Felder werden ignoriert. Ungültige Nachrichten kommen einzeln und
unverändert nach `chat.dlq`; gültige Nachbarn werden trotzdem gespeichert.

## Datenmodell

`message`: `id UUID PRIMARY KEY`, `room_id UUID NOT NULL`, `sender_id VARCHAR(255)
NOT NULL`, `sender_name VARCHAR(255) NOT NULL`, `content TEXT NOT NULL`,
`sent_at TIMESTAMPTZ NOT NULL`. Index `(room_id, sent_at DESC)` für den vorhandenen
Lesepfad. Schema entsteht durch `db/01-schema.sql` beim ersten PostgreSQL-Start.
Kein Fremdschlüssel auf einen Raum: Raumverwaltung ist ausserhalb dieser Bewertung,
und beliebige gültige Raum-UUIDs aus dem Testsendeweg müssen gespeichert werden.
Die alten Raumtabellen bleiben für den bisherigen Demo-Verlauf erhalten.

Der Bewertungsstack verwendet ein neues Volume `postgres-batch-data`, um ein altes
Bootstrap-Schema nicht stillschweigend weiterzuverwenden oder Altdaten zu löschen.
Demodaten sind keine Testnachrichten; Tests zählen ihre eigenen UUIDs/Raum-IDs.

## Bündeln, Transaktionen und Bestätigung

- Pro Instanz ein Consumer, bis zu 100 Nachrichten und höchstens etwa 200 ms
  Sammelzeit. Prefetch 100, damit ein voller Stapel möglich ist.
- Ein `JdbcTemplate.batchUpdate` pro Stapel innerhalb einer Datenbanktransaktion;
  JDBC-Option `reWriteBatchedInserts=true` reduziert Netzwerk-Roundtrips.
- `INSERT ... ON CONFLICT (id) DO NOTHING`: dieselbe ID erzeugt nur eine Zeile.
  Bei abweichendem Inhalt gewinnt die erste erfolgreich gespeicherte Nachricht.
- ACK erst, wenn der Transaktionsaufruf erfolgreich zurückkehrt, also nach COMMIT.
  Absturz vor ACK führt zur erneuten Zustellung; das INSERT bleibt wiederholbar.
- At-least-once, keine behauptete Exactly-once-Zustellung und keine globale
  Reihenfolge bei zwei Instanzen. Der Sendezeitpunkt bleibt unverändert.

1000 wartende Nachrichten benötigen normalerweise 10 Schreibtransaktionen und
müssen inklusive Mess-/Betriebsnebenkosten unter der Grenze von 100 bleiben.
Bei 1667 Nachrichten/s ergeben sich bei vollen 100er-Stapeln etwa 17 Transaktionen/s.
Das ist eine Planungsrechnung, kein ungemessener Durchsatznachweis.

## Fehlerverhalten

| Fall | Verhalten und Begründung |
|---|---|
| Writer aus | Queue hält persistente Nachrichten; nach Start wird der Rückstau verarbeitet. |
| Doppelte Nachricht | Konflikt auf Primärschlüssel wird ignoriert, ACK nach Commit, keine DLQ. |
| PostgreSQL aus / Verbindung abgebrochen | Transaktion scheitert, kein ACK. Nach 2 s NACK mit requeue; automatische Wiederholung ohne manuelles Neustarten. |
| Ausfall genau nach Commit | Wiederholung ist möglich; UUID verhindert doppelte Zeilen. |
| Ungültiges JSON / Pflichtfeld | Nur diese Nachricht wird ohne requeue abgelehnt und landet in `chat.dlq`. |
| Dauerhafter Datenfehler im Stapel | Rollback, einmaliger Versuch pro Zeile; nur fehlerhafte Zeilen in DLQ. |
| Unerwarteter Schreibfehler | Log und requeue mit Pause statt Datenverlust; Betriebsfehler muss behoben werden. |
| RabbitMQ aus / Consumer-Absturz | Spring verbindet erneut; unbestätigte Nachrichten werden erneut zugestellt. |

Die Classic-Queue hat bewusst kein Zustelllimit: ein Infrastruktur-Ausfall darf
gültige Nachrichten nicht nach wenigen Versuchen verwerfen. Keine TTL, keine
Längenbegrenzung mit Verwerfen. DLQ über RabbitMQ-Dead-Letter-Routing an den
Default-Exchange, Schlüssel `chat.dlq`. Durable Queues und Broker-Volume erhalten
Daten bei normalen Container-Neustarts. Ein gleichzeitiger Verlust von Broker und
Volume ist nicht abgesichert; keine Behauptung einer clusterweiten Ausfallsicherheit.

## Konfiguration und Betrieb

Java 21, Spring Boot 3.5.16; Maven-Elternprojekt mit `chat-service` und `batch-writer`.
Compose-Dienste `postgres`, `rabbitmq`, `chat-service`, `batch-writer` liegen im
Netz `chat-net`. Kein Dienst hat `ports` oder einen festen Writer-Containernamen.
Die Container verwenden die Dienstnamen als Hostnamen.

| Variable | Beispiel / Standard | Bedeutung |
|---|---|---|
| POSTGRES_USER | chat | DB-Benutzer |
| POSTGRES_PASSWORD | chat-local-only | DB-Passwort (lokal in `.env`) |
| POSTGRES_DB | chat | Datenbank |
| POSTGRES_HOST | postgres in Compose, localhost lokal | DB-Adresse |
| POSTGRES_PORT | 5432 | DB-Port |
| RABBITMQ_USER | chat | Broker-Benutzer |
| RABBITMQ_PASSWORD | chat-local-only | Broker-Passwort |
| RABBITMQ_HOST | rabbitmq in Compose, localhost lokal | Broker-Adresse |
| RABBITMQ_PORT | 5672 | Broker-Port |
| BATCH_SIZE | 100 | Höchstzahl pro Transaktion und Prefetch |
| BATCH_TIMEOUT_MS | 200 | Zeitlimit der Sammlung |
| RETRY_DELAY_MS | 2000 | Pause vor erneuter Zustellung bei DB-Ausfall |

`.env.example` enthält ausschliesslich Beispielwerte, `.env` steht in `.gitignore`.
DB-Verbindungstimeout 2 s, Sockettimeout 5 s, kein Fail-fast beim Poolstart.
Die PostgreSQL-Daten liegen in einem Volume; Schemaänderungen an bestehenden
Beständen erfordern eine bewusste Migration und werden nicht durch Löschen gelöst.

## Messbare Abnahme

Die geplanten Skriptdateien werden in den folgenden Umsetzungsschritten erstellt.
Voraussetzung: Docker Engine, Java 21, Maven und Python 3 für die Abnahme.

| Szenario | Befehl | Bestehensbedingung |
|---|---|---|
| S1 | `mvn clean test` | Alle Module und echte Container-Integrationstests grün. |
| S2 | `.env.example` nach `.env` kopieren; `docker compose up -d --build`; `python scripts/acceptance.py S2` | Alle vier Dienste laufen, keine veröffentlichten Ports. |
| S3 | `python scripts/acceptance.py S3` | 1000 POSTs, IDs innerhalb 60 s gespeichert, Queue inklusive unacked leer. |
| S4 | `python scripts/acceptance.py S4` | Writer stoppen, 1000 senden, Writer starten; alle IDs gespeichert, DB-Commit-Differenz höchstens 100. |
| S5 | `python scripts/acceptance.py S5` | Zweimal identisches JSON nur mit content_type, genau eine Zeile, kein DLQ-Zuwachs. |
| S6 | `python scripts/acceptance.py S6` | Zwei Writer-Consumer, 1000 eindeutige IDs vollständig gespeichert. |
| S7 | `python scripts/acceptance.py S7` | DB 15 s aus, 300 POSTs; alle innerhalb 90 s in DB, Writer ohne manuellen Neustart. |
| S8 | `python scripts/acceptance.py S8` | Keine Streams, Kommentare über Klassen/Methoden, `.env` nicht versioniert. |

`python scripts/acceptance.py all` führt S2–S8 nacheinander auf demselben Stack aus,
ohne zwischen Szenarien Daten zu löschen. S8 enthält auch eine manuelle Codeprüfung;
ein Textscanner kann die Qualität der Kommentare nicht beurteilen.
Messung innerhalb des Netzes mit `docker compose exec`, SQL und `rabbitmqctl`.
S4 zählt `pg_stat_database.xact_commit` inklusive Messabfragen konservativ mit;
Polling wird sparsam ausgeführt. Fehler stoppen das Skript mit Exit-Code ungleich 0.

## Quellen und Entscheidungen

- Bewertungsauftrag vom 24.09.2026, Szenarien S1–S8.
- Bestehender IT3b-Code und der oben genannte Vergleich mit dem Bewertungsformat.
- [Spring AMQP: Consumer-Batches](https://docs.spring.io/spring-amqp/reference/amqp/receiving-messages/de-batching.html).
- Bewusste Abweichung von der alten IT3b-Planung: mehrere Writer erlaubt,
  DLQ-Name `chat.dlq`, keine offenen Ports und 100er-Stapel statt 500.
