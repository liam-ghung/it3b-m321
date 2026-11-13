# Live-Zustellung ganz einfach erklären

## Der Satz zum Einstieg

Unsere App nimmt Chatnachrichten entgegen. RabbitMQ verteilt jede Nachricht
gleichzeitig zur Speicherung und an jede Chat-Service-Instanz. Die Instanzen
schicken sie sofort an die verbundenen Clients im passenden Raum.

## Weg einer Nachricht: POST an A, Empfänger an B

Die Dateien liegen unter `chat-service/src/main/java/ch/benedict/m321/chat/`.

| Datei und Methode | Was passiert und warum? |
|---|---|
| `message/NewMessage.java` | Eingabedaten: Raum, Absender-ID, Name, Inhalt. Noch keine Server-ID oder Sendezeit. Alte Feldnamen bleiben als Alias möglich. |
| `message/MessageController.java`, `send` | Nimmt POST `/messages` entgegen, prüft Pflichtfelder und ruft den Service auf. Antwort 202: angenommen, noch nicht zwingend gespeichert. |
| `message/MessageService.java`, `sendMessage` | Erzeugt UUID und Zeit, baut das unveränderliche `Message` und veröffentlicht es einmal über RabbitTemplate. |
| `rabbit/RabbitConfiguration.java` | Deklariert `chat.messages`, die Speicherqueue und JSON-Konvertierung. |
| `live/LiveDeliveryConfiguration.java` | Bindet `chat.delivery` an `chat.messages`. Jede Instanz hängt mit ihrer eigenen Live-Queue daran. So bekommt auch B eine Kopie. |
| `live/LiveMessageListener.java`, `onMessage` | Liest JSON, prüft die Nachricht, übernimmt sie in den lokalen Puffer und bestätigt danach dem Broker. Ungültige Nachrichten werden nicht endlos wiederholt. |
| `live/LiveMessageBuffer.java`, `accept` | Entfernt abgelaufene Einträge, ignoriert bekannte IDs, speichert die neue Nachricht und verteilt sie an Clients im richtigen Raum. |
| `live/LiveStreamController.java`, `stream` | Hat den SSE-Client bereits für dessen Raum registriert und die offene HTTP-Antwort eingerichtet. |
| `live/LiveStreamClient.java`, `writeTo` | Ein eigener Sender liest die Client-Warteschlange. Er schreibt `id: UUID`, dann `data: JSON`, dann eine Leerzeile und flusht. |

SSE ist eine offen bleibende HTTP-Antwort vom Server zum Client. Der Client muss
nicht ständig nach neuen Nachrichten fragen. Heartbeat-Kommentare halten die
Verbindung aktiv und helfen, abgebrochene Verbindungen zu erkennen. Sie sind
keine Chatnachrichten und besitzen keine Nachrichten-ID.

## Speicherung getrennt erklären

`chat.persist` wartet auf den Batch-Writer. Dieser sammelt Nachrichten und schreibt
sie zusammen in einer Datenbanktransaktion. Erst nach erfolgreichem Commit erfolgt
sein ACK. Die Live-Queue wird dagegen schon nach lokaler Übernahme bestätigt.
Ein Live-ACK bedeutet daher nicht, dass ein Browser die Nachricht gesehen hat.

`MessageService.loadHistory` und `MessageRepository.findLatest` lesen langfristigen
Verlauf aus PostgreSQL. Sie werden beim normalen Senden und bei der kurzen
SSE-Nachlieferung nicht aufgerufen. Deshalb funktioniert live auch bei DB-Ausfall.

## S6: Eine Instanz hängt

In `LiveDeliveryConfiguration.liveQueue` stehen die entscheidenden Grenzen:

- `x-max-length = 999`: höchstens 999 wartende Nachrichten.
- `setPrefetchCount(1)` und ein Consumer: höchstens eine unbestätigte Lieferung.
- Zusammen also höchstens 1000. `drop-head` entfernt bei Überlauf die ältesten
  wartenden Live-Nachrichten, TTL entfernt zu alte wartende Nachrichten.

Die gesunde Instanz hat eine andere Queue und kann weiterarbeiten. Beim Fortsetzen
liefert die pausierte Instanz neue Nachrichten wieder. Sie muss nicht alle alten
5000 Live-Ereignisse nachholen; die dauerhafte Speicherung läuft separat.

Auch lokal gibt es Grenzen: 10000 Einträge im Wiederholungspuffer, 10000 pro Client
und maximal 100 offene Clients. `LiveStreamClient.offer` wartet niemals auf das
Netzwerk. Bei vollem Client-Puffer schließt es nur diesen Client. Socket-Schreiben
läuft im begrenzten Executor aus `LiveStreamConfiguration`.

## S7: Verbindung war unterbrochen

1. Client merkt sich die letzte empfangene ID und sendet sie beim Wiederverbinden
   als Header `Last-Event-ID`.
2. `LiveStreamController.parseCursor` prüft die UUID. Fehlerhaft ergibt 400.
3. `LiveMessageBuffer.register(roomId, lastEventId)` entfernt abgelaufene Einträge
   und prüft, ob der Anker noch vorhanden ist und zum Raum gehört. Sonst 409.
4. Unter derselben Sperre wie `accept` kommen die folgenden Nachrichten dieses
   Raums in die neue Client-Warteschlange, anschließend wird der Client registriert.
5. Neue Nachrichten werden dahinter angehängt. Der Sender schreibt alles in dieser
   Reihenfolge. Der Anker selbst wird nicht noch einmal gesendet.

Die Sperre verhindert eine Lücke zwischen „alte Nachrichten holen“ und „live
anmelden“. Unter ihr wird nicht ins Netzwerk geschrieben. Der Puffer hält maximal
10000 Nachrichten und höchstens 120 Sekunden. Ein unbekannter Cursor bekommt 409,
weil wir sonst einen vollständigen Verlauf versprechen würden, den wir nicht haben.
Ein Instanzwechsel klappt nur, wenn die andere Instanz den Anker ebenfalls besitzt.

## Aufräumen

`unregister` schließt und entfernt einen Client; mehrfaches Aufrufen ist harmlos.
Das geschieht im `finally` des Senders und nach Abschluss der HTTP-Anfrage.
`clear` beendet alle Clients und leert den Zustand bei Broker-Verlust oder Shutdown.
Eine exklusive Auto-delete-Queue verschwindet beim Ende ihrer Broker-Verbindung.
Die dauerhaften Queues `chat.persist` und `chat.dlq` bleiben erhalten.

## Lehrerfragen zum Üben

| Frage | Kurze Antwort |
|---|---|
| Warum eine Queue pro Instanz? | Bei einer gemeinsamen Queue würden Consumer Nachrichten untereinander aufteilen. Für Anzeige braucht jede Instanz jede Nachricht. |
| Warum nur ein Publish? | RabbitMQ verteilt dieselbe Nachricht auf beide Wege. Zwei getrennte Publishes könnten teilweise scheitern. |
| Was ist ACK? | Die Bestätigung an den Broker, dass diese Lieferung verarbeitet wurde. Der Zeitpunkt hängt vom Consumer ab. |
| Warum braucht Replay keine Datenbank? | Die Instanz bewahrt die zuletzt empfangenen Nachrichten kurz im Arbeitsspeicher auf. |
| Warum keine doppelte Nachricht beim Übergang? | Vorbefüllen und Registrieren sind gemeinsam gesperrt; bekannte IDs werden im Empfangspuffer unterdrückt. |
| Was passiert bei vollem Client-Puffer? | Dieser Client wird getrennt, andere Clients und der Broker-Empfang laufen weiter. |
| Was bedeutet 409 hier? | Die letzte ID ist für diesen Raum nicht mehr verfügbar; der Verlauf muss neu geladen werden. |
| Was beweisen unsere Tests? | Die beschriebenen Fälle im geprüften Umfang. Kein unbegrenztes Replay, keine garantierte Zustellung bei beliebig langen Ausfällen. |

Übe die drei Abläufe am eigenen Code: normaler POST bis SSE, Pause und Wiederverbindung.
Das echte mündliche Gespräch kann diese Vorbereitung nicht ersetzen.
