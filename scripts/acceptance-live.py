"""Bewertung 2: S3 und S5 im Compose-Stack; weitere Szenarien folgen in ihren Planschritten."""

import argparse
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

    def __init__(self, address, room):
        """Wartet vor dem Senden auf den geflushten Eroeffnungskommentar."""
        self.connection = http.client.HTTPConnection(address, 8080, timeout=15)
        self.connection.request("GET", "/stream?roomId=" + room)
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


def worker(scenario, addresses):
    """Laeuft ausschliesslich im Hilfscontainer und spricht jede Instanz direkt per IP an."""
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
    try:
        receive(output, "ready", 60)
        if name == "S5":
            stopped = True
            compose("stop", "batch-writer", "postgres")
            running = compose("ps", "--services", "--status", "running").splitlines()
            assert "batch-writer" not in running and "postgres" not in running
        process.stdin.write("go\n")
        process.stdin.flush()
        result = receive(output, "result", 120)["result"]
        assert process.wait(timeout=20) == 0
    finally:
        # Keine Volumes, Queues oder Tabellen loeschen. Nur den eigenen Testclient entfernen.
        try:
            if stopped:
                compose("start", "postgres", "batch-writer")
        finally:
            if process.poll() is None:
                run(["docker", "rm", "-f", helper])
                process.wait(timeout=20)
    if name == "S5":
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
    parser.add_argument("scenario", choices=["S3", "S5"])
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
