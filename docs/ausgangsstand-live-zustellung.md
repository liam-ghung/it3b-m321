# Schritt 1: Ausgangsstand für Live-Zustellung

Prüfdatum: 13.11.2026. Basis: Plan-Commit `0c4fff4` vom 12.11.2026.
Vor Schrittbeginn zeigte `git ls-remote origin refs/heads/main` auf
`0c4fff4f6e0534086c398d694ed51f2487683e79`. Die Planung war vor der Umsetzung gepusht.

## Vorhandener Nachrichtenweg

| Baustein | Nachweis im Code | Verhalten |
|---|---|---|
| HTTP | MessageController.send / history | POST und GET auf /messages sowie /api/messages; POST antwortet 202, ungültige Eingaben 400. |
| Eingabe | NewMessage | roomId, senderId, senderName, content; alte Aliase sender/text bleiben. Ohne Anzeigenamen wird senderId verwendet. |
| Vollständige Nachricht | Message | id, roomId, senderId, senderName, content, sentAt. Keine zusätzlichen JSON-Felder für die alten Java-Zugriffsmethoden. |
| Veröffentlichung | MessageService.sendMessage | UUID und Sendezeit entstehen im Server; ein convertAndSend auf chat.messages mit leerem Routing-Key. Kein Datenbankzugriff beim Senden. |
| JSON | RabbitConfiguration.jsonMessageConverter | Spring-ObjectMapper und Jackson2JsonMessageConverter erzeugen den JSON-Body. |
| Lesen | MessageRepository.findLatest im Chat-Service | SQL nach room_id gefiltert, ORDER BY sent_at DESC, LIMIT; mapRow bildet alle sechs Felder ab. |
| Speichern | BatchListener und MessageRepository im Writer | Stapel in einer Transaktion, ON CONFLICT (id) DO NOTHING; ACK nach erfolgreicher Rückkehr aus saveBatch. |

## Ist-Topologie laut Quelltext

```text
POST -> chat.messages (dauerhafter Fanout-Exchange)
             |
             +-> chat.persist (dauerhafte Queue) -> Batch-Writer -> PostgreSQL
                       |
                       +-> Standard-Exchange -> chat.dlq bei endgültiger Ablehnung
```

RabbitConfiguration.persistBinding bindet chat.persist direkt an chat.messages.
Die Persistenzqueue hat als Dead-Letter-Exchange den leeren Namen und als
Dead-Letter-Routing-Key chat.dlq. Chat-Service und Writer deklarieren dieselben
dauerhaften Queues. Die Abbildung ist aus dem Code abgeleitet, kein behaupteter
Snapshot eines laufenden Compose-Brokers.

Compose definiert vier Dienste ohne ports und ohne container_name. Netzwerkname:
chat-net. chat-service hängt beim initialen Start von gesunder DB und gesundem
Broker ab. Ein späterer DB-Ausfall ist davon zu unterscheiden. .env ist laut
`git ls-files -- .env` nicht versioniert.

## Abgleich mit der Spezifikation

- Bestehender POST-/JSON-Vertrag und Speicherweg entsprechen der beschriebenen Basis.
- chat.delivery, Exchange-Binding, Live-Queues, SSE-Endpunkt, Client-Verwaltung und
  Replay-Puffer sind noch nicht implementiert. Das sind geplante Erweiterungen,
  keine bereits bestandenen Szenarien S3–S7.
- Die bisherige Queue-Bindung darf nicht entfernt werden. Schritt 2 ergänzt den in
  der Spezifikation festgelegten Anzeige-Exchange, damit die S4-Zählung getrennt ist.
- Die aktuelle SQL-Verlaufsabfrage ist absteigend und hat keine Cursor-Auflösung.
  Sie wird deshalb nicht irrtümlich als fertige Last-Event-ID-Nachlieferung benutzt.
- Alte Architekturtexte nennen zum Teil andere Batchgrößen oder Queue-Typen.
  Maßgeblich sind der vorhandene Code aus Bewertung 1 und die neue Live-Spezifikation.
- Die Zusagen über Reihenfolge, Queue-Löschung, Backpressure und Replay sind erst
  durch die späteren Integrationstests zu belegen. Kein Vertragswechsel in Schritt 1.

## Gezielte Absicherung des Vertrags

Die vorhandenen Controller-Tests decken beide POST-Schreibweisen, Validierung und
Verlauf ab. MessageServiceTest prüft Server-ID/-Zeit, genau den bisherigen Publish
und fehlenden Datenbankzugriff. Der neue MessageContractTest sichert ergänzend:

- exakt dieselben sechs Felder und Werte in API-Serialisierung und Broker-Body;
- ISO-8601-Zeit mit Sekundenbruchteilen;
- unveränderte Anführungszeichen, Umlaute und Textzeilenumbrüche nach JSON-Roundtrip;
- JSON ohne rohe CR/LF-Zeichen sowie application/json im Broker-Content-Type.

Der Test verwendet den echten konfigurierten JSON-Konverter, aber keinen Broker.
Er behauptet damit keine Netzwerkzustellung oder SSE-Abnahme. Es wurde kein
Produktionscode geändert und noch keine Live-Zustellung gebaut.

## Testprotokoll

Aufruf im Projektwurzelverzeichnis: `mvn -B -ntp clean test`.
Der erste Lauf bestand im Chat-Service mit 16 Tests; die Writer-Testklasse konnte
wegen nicht erreichbarer Docker Engine nicht starten. Nach Ergänzung des
Vertragstests bestanden 17 Chat-Service-Tests, der Docker-Start war noch ausstehend.
Die fehlende Engine wurde nicht durch Auslassen der Integrationstests umgangen.

Docker Desktop wurde gestartet und anschließend nur die Distribution docker-desktop
neu gestartet, ohne Volumes oder andere Distributionen zu löschen. Die Engine blieb
nicht erreichbar. Das Backend-Protokoll meldete beim Start unter anderem
`dockerd failed to start: starting nfs-common: signal: killed`.

Nach Öffnen von Docker Desktop war die Engine 28.0.1 erreichbar. Der vollständige
Root-Lauf wurde erneut ausgeführt und am 13.11.2026 um 08:28:49 Uhr (Europe/Zurich)
erfolgreich abgeschlossen: BUILD SUCCESS, Dauer rund 62 Sekunden.

| Modul | Tests | Fehler | Fehlgeschlagen | Übersprungen |
|---|---:|---:|---:|---:|
| chat-service | 17 | 0 | 0 | 0 |
| batch-writer | 6 | 0 | 0 | 0 |
| Gesamt | 23 | 0 | 0 | 0 |

Die sechs Writer-Integrationstests liefen mit echtem RabbitMQ und PostgreSQL,
einschließlich Duplikaten, Rückstau, ungültigen Nachrichten und DB-Ausfall.
Verbindungswarnungen beim absichtlich ausgelösten Ausfall und beim Container-Abbau
sind im Log enthalten; die Testberichte melden keine Fehler. Lokales vollständiges
Protokoll: tmp/step1-live-maven-final.log (nicht versioniert).
Schritt 1 ist damit geprüft. S2–S8 der Live-Zustellung sind noch nicht abgenommen.
