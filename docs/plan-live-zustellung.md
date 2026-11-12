# Umsetzungsplan: Live-Zustellung, Bewertung 2

Stand: 12.11.2026. Status: Planung abgeschlossen; Implementierung beginnt frühestens am 13.11.2026.
Der bisherige Plan im Chat war ein Entwurf. Diese Datei ist die erste Projektfassung.

## Grundlage und Startbedingungen

Gleicher Fork wie Bewertung 1; Java 21, Spring Boot, RabbitMQ, PostgreSQL und der
bestehende Batch-Writer bleiben Grundlage. Code auf Englisch, Erklärungen und
Commit-Nachrichten auf Deutsch. Regeln aus CLAUDE.md gelten für chat-service/.
Keycloak, Gateway, React, JavaFX und Raumverwaltung gehören nicht zu diesem Auftrag.

Vor dem ersten Implementierungsschritt:

1. Grundlage ist docs/spec-live-zustellung.md mit Vertrag, Entscheidungen und Abnahme.
2. Spezifikation und Plan gegen S1–S8 abgleichen.
3. Vertragsänderungen während der Umsetzung nicht stillschweigend vornehmen.
4. Spezifikation und Plan versionieren, Plan auf origin/main pushen und den auf GitHub
   sichtbaren Plan-Commit notieren. Erst danach Schritt 1 beginnen.

Der initiale Plan-Commit trägt die Nachricht „docs: Live-Zustellung vor Umsetzung planen“.
Sein Remote-Stand wird nach dem Push kontrolliert; die Existenz dieser Datei allein
ist noch kein Beleg für einen erfolgreichen Push.

## Festgelegte Entscheidungen

Die detaillierte Begründung und Grenzen stehen in der Spezifikation. Die folgenden
Entscheidungen ersetzen die offenen Fragen des noch nicht versionierten Entwurfs:

- chat.messages bleibt Speicher-Einstieg. Ein Exchange-Binding führt eine Kopie
  an chat.delivery; ausschließlich dort hängen die Live-Queues. S4 zählt dort.
- Eine exklusive, flüchtige Auto-delete-Queue je Instanz, ein Consumer, manuelles
  ACK nach lokaler Übernahme. 999 Ready plus Prefetch 1 ergeben höchstens 1000.
  Drop-head und TTL 30 s betreffen nur Live-Nachrichten.
- Sequenzieller Empfang und ein Sender je SSE-Verbindung erhalten die Reihenfolge.
  Langsame Clients werden bei vollem begrenztem Puffer getrennt; kein Netz-I/O
  unter der Zustandssperre. Globale Gleichzeitigkeit wird nicht als Reihenfolge erfunden.
- Wiederholungspuffer je Instanz: höchstens 10000 Nachrichten und 120 s.
  Cursor-Auflösung, Vorbefüllen und Registrierung erfolgen unter derselben Sperre.
  Replay läuft nicht über die verzögert gefüllte Datenbank. Unbekannter oder nicht
  mehr verfügbarer Cursor: 409; kein Versprechen unbegrenzter Wiederholung.
- Testaufruf scripts/acceptance-live.py mit all oder S2 bis S8. Das Skript wird
  schrittweise erstellt, Ergebnisse werden erst nach tatsächlicher Ausführung notiert.

### Prüfpunkte für die Umsetzung

- Vertrag: POST /messages und /api/messages bestehen bereits. JSON enthält id,
  roomId, senderId, senderName, content, sentAt. GET /stream?roomId=… liefert
  text/event-stream mit Nachrichten-UUID als id: und demselben JSON als data:.
- Topologie/S4: Im aktuellen Code hängt chat.persist bereits an chat.messages.
  Zwei zusätzliche Live-Queues ergeben dort insgesamt drei Queues. Das PDF nennt
  genau zwei Queues bei zwei Instanzen, verlangt aber den Erhalt von chat.persist
  und chat.dlq. Die Spezifikation entscheidet deshalb für chat.delivery als
  eigenen Live-Exchange. Den Speicherweg keinesfalls stillschweigend entfernen.
- Live-Queues: Namen, eine Queue und ein Consumer je Instanz, Lebensdauer bei
  Stop/Verbindungsverlust, Wiederanlage nach Verbindungswiederherstellung,
  Überlaufregel und gegebenenfalls TTL festlegen. Löschung innerhalb von 30 s prüfen.
- S6: Festlegen, wie Ready-Nachrichten UND unbestätigte Lieferungen begrenzt werden.
  Eine reine Längeneinstellung ohne Berücksichtigung des Prefetch reicht als
  Nachweis für die Gesamtgrenze nicht. Auch lokale Puffer und langsame SSE-Clients
  müssen begrenzt sein. Gesunde Clients dürfen nicht mit blockiert werden.
- Reihenfolge: Festlegen, was die Reihenfolge eines Absenders bestimmt und wie sie
  vom POST bis zum Stream erhalten bleibt. Gleichzeitige Sendungen und gleiche
  Zeitstempel berücksichtigen; UUIDs allein sind kein chronologischer Cursor.
- S7: Quelle und Reihenfolge der Nachlieferung, Auflösung von Last-Event-ID,
  unbekannte/raumfremde IDs sowie Verzögerung des Batch-Writers festlegen.
  Registrierung, Nachladen und Live-Übergang müssen zusammen lückenfrei sein und
  überlappende Nachrichten anhand ihrer ID deduplizieren. Keine pauschale Garantie
  aus einer einmaligen Datenbankabfrage oder einer festen Schlafpause ableiten.
- Messung: Reproduzierbare Befehle, erwartete Werte und Start-/Endpunkt jeder
  Zeitmessung gehören in die Spezifikation. Die Tests dürfen den Stack zwischen
  S2–S8 nicht zurücksetzen. Zufällige Raum-IDs trennen Testdaten.

## Regeln während der Umsetzung

Pro Schritt erhält die KI genau den unten stehenden Auftrag zusammen mit der
fertigen Spezifikation. Der Lernende liest die Änderungen, führt die Prüfung aus
und erklärt die wichtigsten Stellen. Ein Commit folgt erst nach bestandener
Schrittprüfung. Keine erfundenen Prüfergebnisse oder nachträglich behauptete Freigabe.

Jeder KI-geschriebene Commit erhält eine Co-Authored-By-Zeile mit der tatsächlich
verwendeten KI-Zuordnung. Jeder Schritt wird als eigenes Thema committed.
Das Feld „Geprüft und korrigiert“ enthält nachher Befehl, Ergebnis, Abweichungen,
veränderte oder verworfene KI-Vorschläge und deren Begründung. Falls keine Änderung
nötig war, wird das ausdrücklich festgehalten. Es bleibt bis zur Prüfung offen.

Nach dem initialen Plan-Push bleibt der Plantext erhalten. Nötige Änderungen werden
mit Grund und betroffenen Schritten als Nachtrag in einem eigenen Commit ergänzt,
bevor die betroffene Arbeit beginnt. Ausnahme: die vorgesehenen Prüffelder.
Fremde Arbeitsdateien und der Tag bewertung-1 bleiben unberührt.

## Schritt 1: Ausgangsstand und Testgrundlage

**Ziel / Reihenfolge:** Bestehenden Speicherweg und Vertrag vor Erweiterungen sichern.

**Auftrag an die KI im Wortlaut:**
> Prüfe den bestehenden Code gegen meine fertige Spezifikation. Dokumentiere die
> relevanten Klassen, den JSON-Vertrag und die Exchange-Bindings. Führe die bestehenden
> Root-Tests aus. Ergänze bei Bedarf gezielte Regressionstests für den bestehenden
> Nachrichtenvertrag. Implementiere noch keine Live-Zustellung. Melde Widersprüche,
> statt den festgelegten Vertrag eigenständig zu verändern.

**Test:** mvn clean test im Wurzelverzeichnis, alle Module erfolgreich. Ergebnis und
Ist-Topologie dokumentieren. Kein bestimmtes Testergebnis wird vorweggenommen.

**Commit:** docs: Ausgangsstand für Live-Zustellung absichern

**Geprüft und korrigiert:** Offen.

## Schritt 2: Live-Queue und Empfang pro Instanz

**Ziel / Reihenfolge:** Erst die unabhängige Verteilung herstellen, dann SSE anbinden.

**Auftrag an die KI im Wortlaut:**
> Implementiere die in meiner Spezifikation festgelegte Live-Topologie mit einer
> Queue und einem Consumer pro Instanz. Setze Queue-Lebensdauer, Grenzen, Prefetch,
> Überlauf und Wiederherstellung um. Erhalte den Speicherweg aus Bewertung 1.
> Ergänze Integrationstests gegen echtes RabbitMQ für zwei unabhängige Instanzen,
> Zustellung derselben Nachricht an beide und das Entfernen einer Instanz.

**Tests:** Root-Maven-Lauf; je eine Kopie an beide Consumer; jede Live-Queue hat einen
Consumer; beim Entfernen einer Instanz verschwindet deren Queue innerhalb von 30 s.
Persistenz- und Fehlerqueue bleiben. Prüfung gemäß zuvor geklärter S4-Zählung.

**Commit:** feat: Live-Nachrichten an jede Chat-Service-Instanz verteilen

**Geprüft und korrigiert:** Offen.

## Schritt 3: SSE und Raumfilterung

**Ziel / Reihenfolge:** Empfangene Nachrichten an die passenden offenen Verbindungen liefern.

**Auftrag an die KI im Wortlaut:**
> Implementiere GET /stream?roomId=… nach meiner Spezifikation. Verwalte Verbindungen
> je Raum, sende id und JSON-data im geforderten SSE-Format und entferne geschlossene
> oder fehlerhafte Verbindungen. Entkopple langsame Clients nach der spezifizierten
> begrenzten Pufferstrategie vom RabbitMQ-Empfang und von anderen Clients. Bewahre
> die festgelegte Reihenfolge. Ergänze Tests für Format, Raumfilterung und Aufräumen.

**Tests:** Root-Maven-Lauf; zwei Clients in A erhalten die Nachricht, B nicht;
Content-Type, id und JSON stimmen. Abbruch räumt Ressourcen auf. Ein langsamer
Client verhindert keine Zustellung an einen gesunden Client.

**Commit:** feat: Raumbezogene Live-Zustellung über SSE ergänzen

**Geprüft und korrigiert:** Offen.

## Schritt 4: Verteilung und unabhängigen Speicherweg nachweisen

**Ziel / Reihenfolge:** Den kompletten Normalfall mit zwei Instanzen prüfen, bevor
Nachlieferung hinzukommt. Testdaten über echte POST-Anfragen senden.

**Auftrag an die KI im Wortlaut:**
> Implementiere die reproduzierbaren Prüfungen für S3 und S5. Verbinde SSE-Clients
> gezielt mit zwei einzelnen Instanzen. Prüfe IDs, Anzahl, Raumfilter und Reihenfolge
> pro Absender, nicht nur die Summe der Ereignisse. Prüfe Live-Zustellung ohne
> laufenden Writer und PostgreSQL sowie spätere Speicherung. Korrigiere gefundene
> Implementierungsfehler; Vertragsänderungen benötigen vorher einen Plan-Nachtrag.

**Tests:** Mindestens ein automatischer Zwei-Instanzen-Test unter chat-service/src/test/
mit echtem RabbitMQ, im Root-Maven-Lauf. S3: je 200 POSTs pro Instanz, beide A-Clients
erhalten dieselben 400 IDs genau einmal in Absenderreihenfolge, B keine. S5: Writer
und PostgreSQL stoppen, 100 senden, alle innerhalb des geforderten 5-s-Limits live;
nach Neustart alle 100 in der Datenbank. Beginn/Ende der Messung aus der Spezifikation.

**Commit:** test: Verteilung und Live-Zustellung ohne Datenbank prüfen

**Geprüft und korrigiert:** Offen.

## Schritt 5: Rückstau unter Pause prüfen

**Ziel / Reihenfolge:** Die bereits implementierten Grenzen unter echter Pause messen.

**Auftrag an die KI im Wortlaut:**
> Erstelle die S6-Prüfung mit zwei Chat-Service-Instanzen und docker compose pause.
> Sende 5000 Nachrichten über die gesunde Instanz und beobachte den Rückstau während
> des gesamten Versuchs. Prüfe Ready und Unacked, lokale Grenzen und die vollständige
> Zustellung am gesunden Client. Setze die pausierte Instanz fort und prüfe neue
> Nachrichten. Korrigiere Fehler innerhalb der festgelegten Rückstauentscheidung.

**Tests:** Root-Maven-Lauf und S6; Gesamtqueue-Grenze höchstens 1000 unter dem
festgelegten Messverfahren; gesunder Client erhält alle 5000 IDs; nach Fortsetzen
liefert auch die andere Instanz neue Nachrichten. Tatsächliche Höchstwerte notieren.

**Commit:** test: Begrenzten Rückstau und Wiederanlauf nach Pause prüfen

**Geprüft und korrigiert:** Offen.

## Schritt 6: Nachlieferung und Übergang zu live

**Ziel / Reihenfolge:** Den bestehenden Live-Weg um den zuvor spezifizierten
Wiederverbindungsalgorithmus erweitern.

**Auftrag an die KI im Wortlaut:**
> Implementiere Last-Event-ID genau nach meiner Spezifikation. Verbinde Registrierung,
> Nachlieferung und Live-Übergang so, dass überlappende Nachrichten nicht doppelt
> erscheinen und gleichzeitig eintreffende nicht verloren gehen. Berücksichtige
> verzögerte Speicherung, Cursor-Auflösung, Raumgrenzen und unbekannte IDs. Ergänze
> Integrationstests mit echtem RabbitMQ für S7 und die Übergangs-Rennbedingungen.

**Tests:** Root-Maven-Lauf, mindestens ein Neuverbindungs-Test unter chat-service/src/test/.
S7: nach einer Nachricht trennen, 50 senden, nach 10 s mit Last-Event-ID verbinden;
genau 50 fehlende IDs in Reihenfolge, danach neue Nachrichten live. Zusätzlich
Nachrichten während des Nachladens sowie verzögerte DB-Sichtbarkeit testen.
Unbekannte und raumfremde IDs gemäß Vertrag behandeln.

**Commit:** feat: Nachrichten nach SSE-Neuverbindung lückenfrei nachliefern

**Geprüft und korrigiert:** Offen.

## Schritt 7: Reproduzierbare Gesamtabnahme und Abgabe

**Ziel / Reihenfolge:** Abschließend das Zusammenspiel aller Schritte aus dem
versionierten Stand prüfen und die tatsächlichen Ergebnisse dokumentieren.

**Auftrag an die KI im Wortlaut:**
> Vervollständige die reproduzierbare Gesamtabnahme für S1–S8 mit den Befehlen aus
> meiner Spezifikation. Aktualisiere die README-Tabelle Stand und .env.example für
> neue Variablen. Prüfe den gesamten chat-service einschließlich Tests gegen
> CLAUDE.md. Führe den Root-Maven-Lauf und S2–S8 in vorgegebener Reihenfolge ohne
> Zurücksetzen des Stacks aus. Prüfe den Build aus einem frischen Klon des
> versionierten Kandidaten. Halte Messwerte und Grenzen ehrlich fest. Erstelle
> eine Orientierung zu den Codestellen für den normalen Weg, S6 und S7.

**Tests:** S1: mvn clean test. S2: frischer Klon, .env nur aus .env.example,
docker compose up -d --build, alle Dienste laufen ohne Host-Portbindungen.
S3–S7 nacheinander auf demselben Stack, mit Einzelinstanz-Verbindungen im chat-net.
S8: Kommentare über allen Klassen/Methoden, keine Streams, .env nicht getrackt.
Gesamtabnahme ersetzt nicht die echten RabbitMQ-Tests im Maven-Lauf.

**Commit:** docs: Gesamtabnahme und Code-Review-Vorbereitung dokumentieren

**Geprüft und korrigiert:** Offen.

Für einen echten frischen Klon kann zuerst der geprüfte Kandidat committed werden;
das erst danach verfügbare Klon-Ergebnis wird als gesonderter Dokumentations-Commit
festgehalten. Bei Fehlern kein Tag: Korrektur und betroffene Prüfungen wiederholen.

## Abschluss

Nach bestandener Abnahme main pushen, bewertung-2 auf den geprüften Abschluss setzen
und pushen; vorhandenen Tag nicht stillschweigend verschieben. Remote-Commit und
Tag-Ziel kontrollieren. Den Fork-Link reicht der Lernende selbst ein.
Für das Gespräch erklärt er POST bis SSE sowie S6 und S7 selbst Zeile für Zeile.

## Begründete Planänderungen nach dem initialen Push

Noch keine. Künftige Einträge enthalten Datum, Grund, betroffene Schritte und Änderung.
