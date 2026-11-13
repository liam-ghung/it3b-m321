"""Bewertung 2: S3, S5, S6 und S7 im Compose-Stack; weitere Szenarien folgen in ihren Planschritten."""

import argparse
import base64
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import queue
import shlex
import socket
import subprocess
import sys
import threading
import time
import uuid
import urllib.parse

ROOT = Path(__file__).resolve().parent.parent
PROJECT = None
IMAGE = "python:3.13-alpine"


def run(arguments, timeout=180):
    """Fuehrt nur Argumentlisten aus; Fehler werden nicht als leere Messwerte behandelt."""
    result = subprocess.run(arguments, cwd=ROOT, capture_output=True, text=True,
                            encoding="utf-8", errors="replace", timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stderr[-3000:] or result.stdout[-3000:])
    return result.stdout.strip()


def compose(*arguments):
    """Verwendet den ausgewaehlten Compose-Stack samt dessen vorhandener env-Datei."""
    command = ["docker", "compose"]
    if PROJECT:
        command += ["-p", PROJECT]
    return run(command + list(arguments))


def eventually(check, seconds=60):
    """Wartet begrenzt auf Wiederanlauf; der letzte Fehler bleibt bei Timeout sichtbar."""
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            last_error = error
        time.sleep(1)
    raise AssertionError(f"Zeitlimit {seconds}s; letzter Fehler: {last_error}")


class Stream:
    """Ein echter SSE-Leser im Docker-Netz sammelt vollstaendige Ereignisse samt Empfangszeit."""

    def __init__(self, address, room, last_event_id=None):
        """Wartet vor dem Senden auf den geflushten Eroeffnungskommentar."""
        self.connection = http.client.HTTPConnection(address, 8080, timeout=15)
        headers = {}
        if last_event_id is not None:
            headers["Last-Event-ID"] = last_event_id
        self.connection.request("GET", "/stream?roomId=" + room, headers=headers)
        self.response = self.connection.getresponse()
        assert self.response.status == 200, self.response.status
        assert self.response.getheader("Content-Type", "").startswith("text/event-stream")
        self.socket = self.connection.sock
        self.events = queue.Queue(maxsize=10000)
        self.opened = threading.Event()
        self.error = None
        self.closed = False
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()
        assert self.opened.wait(10), "SSE-Eroeffnung fehlt"

    def read(self):
        """Kommentare sind keine Daten; jede leere Zeile schliesst einen SSE-Rahmen ab."""
        event_id = None
        data = None
        try:
            while not self.closed:
                raw = self.response.readline()
                if not raw:
                    raise RuntimeError("SSE unerwartet beendet")
                line = raw.decode("utf-8").rstrip("\r\n")
                if line.startswith(":"):
                    self.opened.set()
                elif line.startswith("id: "):
                    event_id = line[4:]
                elif line.startswith("data: "):
                    assert data is None, "Mehrere data-Zeilen"
                    data = json.loads(line[6:])
                elif not line:
                    if data is not None:
                        assert event_id == data["id"], "SSE-ID und JSON-ID verschieden"
                        self.events.put_nowait((data, time.monotonic()))
                    event_id = None
                    data = None
        except Exception as error:
            if not self.closed:
                self.error = error

    def close(self):
        """Beendet auch einen auf die naechste Zeile wartenden Testleser."""
        self.closed = True
        if self.socket:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.response.close()
        self.connection.close()
        self.reader.join(2)


def send(address, room, sender, count):
    """Sendet je Absender sequenziell und merkt POST-Beginn sowie die komplette Antwort."""
    expected = []
    connection = http.client.HTTPConnection(address, 8080, timeout=10)
    try:
        for index in range(count):
            body = {"roomId": room, "senderId": sender, "senderName": sender,
                    "content": f'Nachricht {index}: "Gruesse"\nzweite Zeile'}
            started = time.monotonic()
            connection.request("POST", "/messages", json.dumps(body), {"Content-Type": "application/json"})
            response = connection.getresponse()
            payload = response.read()
            assert response.status == 202, (response.status, payload[:200])
            expected.append((json.loads(payload), started))
        return expected, time.monotonic()
    finally:
        connection.close()


def verify(stream, expected, deadline):
    """Prueft IDs, kompletten Inhalt, Duplikate und Absenderfolge innerhalb einer gemeinsamen Frist."""
    remaining = {message["id"]: message for message, started in expected}
    assert len(remaining) == len(expected), "Doppelte POST-ID"
    wanted_order = {}
    actual_order = {}
    for message, started in expected:
        wanted_order.setdefault(message["senderId"], []).append(message["id"])
    received = {}
    for index in range(len(expected)):
        left = deadline - time.monotonic()
        assert left > 0, "Zustellfrist ueberschritten"
        try:
            message, arrived = stream.events.get(timeout=left)
        except queue.Empty:
            raise AssertionError(f"Nachrichten fehlen; Leserfehler: {stream.error}")
        assert arrived <= deadline, "Nachricht zu spaet"
        assert message == remaining.pop(message["id"], None), "Fremde, doppelte oder veraenderte Nachricht"
        actual_order.setdefault(message["senderId"], []).append(message["id"])
        received[message["id"]] = arrived
    assert not remaining
    assert actual_order == wanted_order, "Absenderreihenfolge falsch"
    return received


def emit(value):
    """Das Hostprogramm liest genau eine JSON-Antwort je abgeschlossener Phase."""
    print(json.dumps(value), flush=True)


class QueueMonitor:
    """Misst die pausierte Live-Queue im Netz, ohne pro Stichprobe einen Docker-Prozess zu starten."""

    def __init__(self, credentials, paused_address):
        """Ordnet die Queue ueber den echten Consumer-Peer zu und prueft die strukturellen Grenzen."""
        token = base64.b64encode(credentials.encode()).decode()
        self.headers = {"Authorization": "Basic " + token}
        consumers = self.get("/api/consumers/%2F")
        matches = [item for item in consumers if item["queue"]["name"].startswith("chat.live.")
                   and item["channel_details"]["peer_host"] == paused_address]
        assert len(matches) == 1, "Pausierte Instanz hat nicht genau einen Live-Consumer"
        consumer = matches[0]
        assert consumer["prefetch_count"] == 1 and consumer["ack_required"]
        self.name = consumer["queue"]["name"]
        self.path = "/api/queues/%2F/" + urllib.parse.quote(self.name, safe="")
        info = self.get(self.path)
        assert info["consumers"] == 1
        assert info["exclusive"] and info["auto_delete"] and not info["durable"]
        arguments = info["arguments"]
        assert arguments["x-max-length"] == 999
        assert arguments["x-message-ttl"] == 30000
        assert arguments["x-overflow"] == "drop-head"
        assert arguments["x-queue-type"] == "classic"
        self.arguments = arguments
        self.samples = []
        self.error = None
        self.stop = threading.Event()
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self.measure, daemon=True)

    def get(self, path):
        """Management-Fehler sind Testfehler; Zugangsdaten werden nie protokolliert."""
        connection = http.client.HTTPConnection("rabbitmq", 15672, timeout=5)
        try:
            connection.request("GET", path, headers=self.headers)
            response = connection.getresponse()
            payload = response.read()
            assert response.status == 200, f"Management HTTP {response.status}"
            return json.loads(payload)
        finally:
            connection.close()

    def measure(self):
        """Ready und Unacked werden gemeinsam erfasst; Management-Werte koennen zeitlich verzoegert sein."""
        try:
            while not self.stop.is_set():
                began = time.monotonic()
                info = self.get(self.path)
                ready = info["messages_ready"]
                unacked = info["messages_unacknowledged"]
                self.samples.append({"seconds": round(time.monotonic() - self.started, 6),
                                     "ready": ready, "unacked": unacked, "total": ready + unacked})
                assert ready <= 999 and unacked <= 1 and ready + unacked <= 1000, "Queue-Grenze verletzt"
                self.stop.wait(max(0, 0.1 - (time.monotonic() - began)))
        except Exception as error:
            self.error = error

    def finish(self):
        """Beendet die Messung vor Wiederaufnahme und gibt Maxima samt allen Stichproben zurueck."""
        self.stop.set()
        self.thread.join(6)
        assert not self.thread.is_alive(), "Queue-Messung haengt"
        assert self.error is None, str(self.error)
        assert self.samples, "Keine Queue-Messung"
        gaps = [self.samples[index]["seconds"] - self.samples[index - 1]["seconds"]
                for index in range(1, len(self.samples))]
        return {"queue": self.name, "arguments": self.arguments, "prefetch": 1,
                "max_ready": max(item["ready"] for item in self.samples),
                "max_unacked": max(item["unacked"] for item in self.samples),
                "max_total": max(item["total"] for item in self.samples),
                "samples": self.samples, "max_poll_gap_seconds": max(gaps, default=0)}


def pause_worker(addresses):
    """Prueft 5000 Live-Ereignisse waehrend der Pause und neue Ereignisse nach dem Fortsetzen."""
    config = json.loads(sys.stdin.readline())
    monitor = QueueMonitor(config["credentials"], addresses[1])
    clients = []
    try:
        room = str(uuid.uuid4())
        clients.append(Stream(addresses[0], room))
        monitor.thread.start()
        emit({"phase": "ready"})
        assert sys.stdin.readline().strip() == "go"
        started = time.monotonic()
        expected, finished = send(addresses[0], room, "healthy-sender", 5000)
        verify(clients[0], expected, finished + 60)
        # Mindestens ein gefuellter Management-Snapshot muss sichtbar werden, bevor wir fortsetzen.
        deadline = time.monotonic() + 10
        while not any(sample["total"] >= 999 for sample in monitor.samples):
            assert monitor.error is None, str(monitor.error)
            assert time.monotonic() < deadline, "Kein voller Rueckstau gemessen"
            time.sleep(0.1)
        time.sleep(1)
        assert clients[0].error is None, str(clients[0].error)
        assert clients[0].events.empty(), "Doppelte Live-Nachricht"
        metrics = monitor.finish()
        metrics.update({"live_messages": 5000, "send_seconds": round(finished - started, 3),
                        "room_id": room, "ids": [message["id"] for message, began in expected]})
        emit({"phase": "resume", "measurements": metrics})
        assert sys.stdin.readline().strip() == "go"
        # Ein frischer Raum trennt den Wiederanlaufnachweis von absichtlich verworfenem altem Rueckstau.
        new_room = str(uuid.uuid4())
        clients.append(Stream(addresses[0], new_room))
        clients.append(Stream(addresses[1], new_room))
        fresh, finished = send(addresses[0], new_room, "after-resume", 3)
        verify(clients[1], fresh, finished + 60)
        verify(clients[2], fresh, finished + 60)
        time.sleep(1)
        for client in clients:
            assert client.error is None, str(client.error)
            assert client.events.empty(), "Zusaetzliches Ereignis nach Wiederaufnahme"
        metrics["new_messages_per_client_after_resume"] = 3
        emit({"phase": "result", "result": metrics})
    finally:
        monitor.stop.set()
        if monitor.thread.is_alive():
            monitor.thread.join(6)
        for client in clients:
            client.close()


def reconnect_worker(addresses):
    """S7 prueft dieselbe und die andere Instanz bei abgeschaltetem Speicherweg."""
    room = str(uuid.uuid4())
    clients = []
    try:
        original = Stream(addresses[0], room)
        clients.append(original)
        emit({"phase": "ready"})
        assert sys.stdin.readline().strip() == "go"
        anchor, finished = send(addresses[0], room, "anchor", 1)
        verify(original, anchor, finished + 5)
        original.close()
        clients.remove(original)
        missed, finished = send(addresses[0], room, "missed", 50)
        time.sleep(10)
        cursor = anchor[0][0]["id"]
        clients.append(Stream(addresses[0], room, cursor))
        clients.append(Stream(addresses[1], room, cursor))
        deadline = time.monotonic() + 60
        for client in clients:
            verify(client, missed, deadline)
        fresh, finished = send(addresses[0], room, "live-again", 1)
        for client in clients:
            verify(client, fresh, finished + 5)
        time.sleep(1)
        for client in clients:
            assert client.error is None, str(client.error)
            assert client.events.empty(), "Anker oder Dublette erneut zugestellt"
        emit({"phase": "result", "result": {
            "replayed_per_client": 50, "new_live_per_client": 1, "reconnect_wait_seconds": 10,
            "reconnect_instances": 2, "storage_stopped_during_replay": True,
            "room_id": room, "ids": [message["id"] for message, began in anchor + missed + fresh]}})
    finally:
        for client in clients:
            client.close()


def worker(scenario, addresses):
    """Laeuft ausschliesslich im Hilfscontainer und spricht jede Instanz direkt per IP an."""
    if scenario == "S6":
        pause_worker(addresses)
        return
    if scenario == "S7":
        reconnect_worker(addresses)
        return
    room = str(uuid.uuid4())
    clients = []
    try:
        clients.append(Stream(addresses[0], room))
        if scenario == "S3":
            clients.append(Stream(addresses[1], room))
            clients.append(Stream(addresses[1], str(uuid.uuid4())))
        emit({"phase": "ready"})
        assert sys.stdin.readline().strip() == "go", "Startsignal fehlt"
        if scenario == "S3":
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(send, addresses[0], room, "sender-a", 200)
                second = pool.submit(send, addresses[1], room, "sender-b", 200)
                expected_a, finished_a = first.result()
                expected_b, finished_b = second.result()
            expected = expected_a + expected_b
            deadline = max(finished_a, finished_b) + 60
            verify(clients[0], expected, deadline)
            verify(clients[1], expected, deadline)
            result = {"messages_per_client": 400, "room_b_messages": 0, "sender_order": True}
        else:
            expected, finished = send(addresses[0], room, "offline-storage", 100)
            received = verify(clients[0], expected, finished + 5)
            latencies = [{"id": message["id"], "seconds": round(received[message["id"]] - started, 6)}
                         for message, started in expected]
            result = {"live_messages": 100,
                      "last_delivery_after_last_post_seconds": round(max(received.values()) - finished, 6),
                      "post_start_to_sse_seconds": latencies}
        # Kurze Ruhephase prueft auch zusaetzliche Ereignisse nach der erwarteten Menge.
        time.sleep(1)
        for client in clients:
            assert client.error is None, str(client.error)
            assert client.events.empty(), "Unerwartete zusaetzliche Nachricht oder falscher Raum"
        result.update({"room_id": room, "ids": [message["id"] for message, started in expected]})
        emit({"phase": "result", "result": result})
    finally:
        for client in clients:
            client.close()


def addresses():
    """Liest konkrete Container-Adressen statt zufaellig ueber den Dienstnamen zu verteilen."""
    containers = compose("ps", "-q", "chat-service").splitlines()
    assert len(containers) == 2, "Genau zwei Chat-Instanzen erforderlich"
    result = []
    for container in containers:
        info = json.loads(run(["docker", "inspect", container]))[0]
        assert not info["HostConfig"]["PortBindings"], "Host-Ports sind nicht erlaubt"
        result.append(info["NetworkSettings"]["Networks"]["chat-net"]["IPAddress"])
        eventually(lambda: run(["docker", "exec", container, "curl", "-fsS", "--max-time", "3",
                               "http://localhost:8080/v3/api-docs"], timeout=5))
    return result


def read_output(process, output):
    """Ein eigener Leser ermoeglicht auf Windows begrenztes Warten auf die Worker-Antwort."""
    for line in process.stdout:
        output.put(line)
    output.put(None)


def receive(output, phase, seconds):
    """Ein abgebrochener Worker oder eine fehlende Phase ist ein Testfehler."""
    line = output.get(timeout=seconds)
    assert line is not None, "Hilfsclient beendet sich ohne Ergebnis"
    value = json.loads(line)
    assert value.get("phase") == phase, value
    return value


def stored(room):
    """Liest nur den frischen Testraum; bestehende Schulungsdaten bleiben unangetastet."""
    uuid.UUID(room)
    statement = f"SELECT id FROM message WHERE room_id = '{room}' ORDER BY id"
    command = 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1 -c '
    command += shlex.quote(statement)
    return compose("exec", "-T", "postgres", "sh", "-c", command).splitlines()


def scenario(name):
    """Oeffnet Streams vor dem Ausfall und stellt gestoppte Dienste auch bei Fehlern wieder her."""
    compose("up", "-d", "--scale", "chat-service=2", "--scale", "batch-writer=1")
    targets = addresses()
    helper = "live-check-" + uuid.uuid4().hex
    script = str(Path(__file__).resolve())
    command = ["docker", "run", "--rm", "-i", "--name", helper, "--network", "chat-net",
               "--mount", f"type=bind,source={script},target=/acceptance-live.py,readonly",
               IMAGE, "python", "/acceptance-live.py", name, "--worker", "--addresses"] + targets
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               text=True, encoding="utf-8", cwd=ROOT)
    output = queue.Queue()
    threading.Thread(target=read_output, args=(process, output), daemon=True).start()
    stopped = False
    paused = None
    try:
        if name == "S6":
            containers = compose("ps", "-q", "chat-service").splitlines()
            for container in containers:
                info = json.loads(run(["docker", "inspect", container]))[0]
                if info["NetworkSettings"]["Networks"]["chat-net"]["IPAddress"] == targets[1]:
                    paused = container
                    environment = dict(item.split("=", 1) for item in info["Config"]["Env"] if "=" in item)
            assert paused, "Zielcontainer fehlt"
            credentials = environment["RABBITMQ_USER"] + ":" + environment["RABBITMQ_PASSWORD"]
            # Zugangsdaten nur ueber stdin, nicht in Prozessargumenten, Logs oder Report.
            process.stdin.write(json.dumps({"credentials": credentials}) + "\n")
            process.stdin.flush()
        receive(output, "ready", 60)
        if name in ("S5", "S7"):
            stopped = True
            compose("stop", "batch-writer", "postgres")
            running = compose("ps", "--services", "--status", "running").splitlines()
            assert "batch-writer" not in running and "postgres" not in running
        if name == "S6":
            # compose pause chat-service wuerde BEIDE Replikate anhalten; hier genau eine Container-ID.
            run(["docker", "pause", paused])
            assert run(["docker", "inspect", "--format", "{{.State.Paused}}", paused]) == "true"
        process.stdin.write("go\n")
        process.stdin.flush()
        if name == "S6":
            receive(output, "resume", 180)
            run(["docker", "unpause", paused])
            assert run(["docker", "inspect", "--format", "{{.State.Paused}}", paused]) == "false"
            paused = None
            process.stdin.write("go\n")
            process.stdin.flush()
        result = receive(output, "result", 120)["result"]
        assert process.wait(timeout=20) == 0
    finally:
        # Keine Volumes, Queues oder Tabellen loeschen. Nur den eigenen Testclient entfernen.
        try:
            if paused:
                state = run(["docker", "inspect", "--format", "{{.State.Paused}}", paused])
                if state == "true":
                    run(["docker", "unpause", paused])
            if stopped:
                compose("start", "postgres", "batch-writer")
        finally:
            if process.poll() is None:
                run(["docker", "rm", "-f", helper])
                process.wait(timeout=20)
    if name in ("S5", "S7"):
        started = time.monotonic()
        expected = sorted(result["ids"])
        eventually(lambda: stored(result["room_id"]) == expected, seconds=60)
        result["stored_messages"] = len(expected)
        result["storage_wait_after_restart_seconds"] = round(time.monotonic() - started, 3)
    result["instance_addresses"] = targets
    return result


def main():
    """Speichert echte Ergebnisse und Fehler; noch nicht implementierte Szenarien werden nicht behauptet."""
    global PROJECT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["S3", "S5", "S6", "S7"])
    parser.add_argument("--project", default=os.environ.get("COMPOSE_PROJECT_NAME"))
    parser.add_argument("--report")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--addresses", nargs="+", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args.scenario, args.addresses)
        return
    PROJECT = args.project
    report = ROOT / (args.report or f"tmp/live-{args.scenario}-result.json")
    result = {"scenario": args.scenario, "success": False}
    try:
        result["measurements"] = scenario(args.scenario)
        result["success"] = True
        print(f"{args.scenario}: bestanden", flush=True)
    except Exception as error:
        result["error"] = str(error)
        raise
    finally:
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
