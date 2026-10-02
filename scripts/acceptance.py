"""Prueft S2 bis S8 im laufenden Compose-Stack; keine Datenbereinigung zwischen Szenarien."""

import argparse
import datetime
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
PROJECT = None


def run(arguments, input_text=None, timeout=180):
    """Fuehrt Befehle ohne Host-Shell aus und bricht bei Fehlern nachvollziehbar ab."""
    result = subprocess.run(arguments, cwd=ROOT, input=input_text, capture_output=True,
                            text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr[-3000:] or result.stdout[-3000:])
    return result.stdout.strip()


def compose(*arguments, input_text=None, timeout=180):
    """Alle Szenarien verwenden denselben benannten Stack und dieselbe env-Datei."""
    command = ["docker", "compose"]
    if PROJECT:
        command += ["-p", PROJECT]
    command += list(arguments)
    return run(command, input_text, timeout)


def sql(statement):
    """Misst innerhalb des Postgres-Containers; Passwoerter erscheinen nicht im Aufruf."""
    command = 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -v ON_ERROR_STOP=1 -c '
    command += shlex.quote(statement)
    return compose("exec", "-T", "postgres", "sh", "-c", command, timeout=15)


def queues():
    """Erfasst auch unbestaetigte Nachrichten, nicht nur den sichtbaren Ready-Rueckstau."""
    output = compose("exec", "-T", "rabbitmq", "rabbitmqctl", "list_queues",
                     "name", "messages", "consumers", "--formatter", "json")
    rows = json.loads(output[output.index("["):])
    return {row["name"]: row for row in rows}


def eventually(check, seconds=60, interval=2):
    """Wiederholt eine Messung bis zum Zeitlimit; ein Timeout ist ein echter Fehler."""
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            last_error = error
        time.sleep(interval)
    raise AssertionError(f"Zeitlimit {seconds}s ueberschritten; letzter Fehler: {last_error}")


def send_messages(count):
    """Sendet echte HTTP-Anfragen innerhalb des Netzes und sammelt die Server-UUIDs."""
    room_id = str(uuid.uuid4())
    body = json.dumps({"roomId": room_id, "senderId": "acceptance",
                       "senderName": "Abnahmetest", "content": "Bewertungstest"})
    command = 'set -eu; i=0; while [ "$i" -lt ' + str(count) + ' ]; do '
    command += 'curl -fsS --max-time 10 -w "|%{http_code}\\n" '
    command += '-H "Content-Type: application/json" --data ' + shlex.quote(body)
    command += ' http://localhost:8080/messages; i=$((i+1)); done'
    output = compose("exec", "-T", "chat-service", "sh", "-c", command)
    ids = []
    for line in output.splitlines():
        payload, status = line.rsplit("|", 1)
        assert status == "202", f"Unerwarteter HTTP-Status {status}"
        message = json.loads(payload)
        ids.append(message["id"])
    assert len(ids) == count and len(set(ids)) == count, "HTTP-Antworten fehlen oder IDs sind doppelt"
    return room_id, set(ids)


def stored_ids(room_id):
    """Vergleicht konkrete IDs statt eine durch Demodaten verfaelschte Gesamtzahl."""
    output = sql(f"SELECT id FROM message WHERE room_id = '{room_id}'")
    return set(output.splitlines())


def wait_for_delivery(room_id, ids, seconds=60):
    """Erst alle erwarteten IDs und eine leere Queue gelten zusammen als Erfolg."""
    started = time.monotonic()
    eventually(lambda: stored_ids(room_id) == ids, seconds)
    remaining = max(1, seconds - (time.monotonic() - started))
    eventually(lambda: queues()["chat.persist"]["messages"] == 0, remaining)
    return round(time.monotonic() - started, 2)


def s2():
    """Prueft einen gestarteten Stack ohne Host-Ports und wartet auf die API."""
    config = json.loads(compose("config", "--format", "json"))
    expected = {"postgres", "rabbitmq", "chat-service", "batch-writer"}
    assert expected.issubset(config["services"])
    for service in config["services"].values():
        assert not service.get("ports"), "Ein Dienst veroeffentlicht Ports"
    running = set(compose("ps", "--services", "--status", "running").splitlines())
    assert expected.issubset(running), f"Dienste fehlen: {expected - running}"
    eventually(lambda: '"status":"UP"' in compose("exec", "-T", "chat-service", "curl",
                "-fsS", "--max-time", "5", "http://localhost:8080/actuator/health"))
    for container in compose("ps", "-q").splitlines():
        bindings = json.loads(run(["docker", "inspect", "--format", "{{json .HostConfig.PortBindings}}", container]))
        assert not bindings, "Container veroeffentlicht Ports"
    return {"services": sorted(running), "published_ports": 0}


def s3():
    """S3: Tausend POSTs muessen vollstaendig und zeitnah gespeichert werden."""
    room_id, ids = send_messages(1000)
    elapsed = wait_for_delivery(room_id, ids)
    return {"stored": len(ids), "drain_seconds": elapsed}


def commits():
    """Zaehlt konservativ alle DB-Commits einschliesslich der Messabfragen."""
    return int(sql("SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()"))


def s4():
    """S4: Ein vorher aufgebauter Rueckstau zeigt die Wirkung der Stapeltransaktionen."""
    compose("stop", "batch-writer")
    try:
        room_id, ids = send_messages(1000)
        assert queues()["chat.persist"]["messages"] == 1000
        before = commits()
    finally:
        compose("start", "batch-writer")
    elapsed = wait_for_delivery(room_id, ids)
    time.sleep(1)
    transaction_count = commits() - before
    assert transaction_count <= 100, f"Zu viele Transaktionen: {transaction_count}"
    return {"stored": len(ids), "transactions_including_probes": transaction_count, "drain_seconds": elapsed}


def s5():
    """S5: Direkte JSON-Zustellung ohne Java-Header prueft den oeffentlichen Vertrag."""
    before_dlq = queues()["chat.dlq"]["messages"]
    room_id = str(uuid.uuid4())
    message_id = str(uuid.uuid4())
    message = {"id": message_id, "roomId": room_id, "senderId": "duplicate",
               "senderName": "Duplikat", "content": "Zweimal gesendet",
               "sentAt": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    request = {"properties": {"content_type": "application/json"}, "routing_key": "chat.persist",
               "payload": json.dumps(message), "payload_encoding": "string"}
    command = 'curl -fsS -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" '
    command += '-H "Content-Type: application/json" --data ' + shlex.quote(json.dumps(request))
    command += ' http://rabbitmq:15672/api/exchanges/%2F/amq.default/publish'
    for index in range(2):
        result = json.loads(compose("exec", "-T", "chat-service", "sh", "-c", command))
        assert result["routed"]
    wait_for_delivery(room_id, {message_id})
    assert queues()["chat.dlq"]["messages"] == before_dlq
    return {"sent": 2, "stored": 1, "dlq_increase": 0}


def s6():
    """S6: Zwei Consumer muessen gleichzeitig an derselben Queue haengen."""
    compose("up", "-d", "--no-deps", "--scale", "batch-writer=2", "batch-writer")
    eventually(lambda: queues()["chat.persist"]["consumers"] == 2)
    room_id, ids = send_messages(1000)
    elapsed = wait_for_delivery(room_id, ids)
    return {"consumers": queues()["chat.persist"]["consumers"], "stored": len(ids), "drain_seconds": elapsed}


def writer_starts():
    """Ein unveraenderter Startzeitpunkt belegt, dass die Writer nicht neu gestartet wurden."""
    result = {}
    for container in compose("ps", "-q", "batch-writer").splitlines():
        result[container] = run(["docker", "inspect", "--format", "{{.State.StartedAt}}", container])
    return result


def s7():
    """S7: Stop/Start der echten Datenbank mit zwei weiterhin laufenden Writern."""
    before = writer_starts()
    compose("stop", "-t", "1", "postgres")
    started = time.monotonic()
    try:
        room_id, ids = send_messages(300)
        time.sleep(max(0, 15 - (time.monotonic() - started)))
    finally:
        compose("start", "postgres")
    remaining = max(1, 90 - (time.monotonic() - started))
    wait_for_delivery(room_id, ids, remaining)
    assert writer_starts() == before, "Ein Writer wurde waehrend des Ausfalls neu gestartet"
    return {"stored": len(ids), "total_seconds": round(time.monotonic() - started, 2), "writer_restarts": 0}


def s8():
    """S8: Automatische Grundpruefung; die Kommentarqualitaet bleibt Teil des Code-Reviews."""
    tracked = run(["git", "ls-files", "--", ".env"])
    assert not tracked, ".env ist versioniert"
    checked = 0
    for path in (ROOT / "batch-writer/src").rglob("*.java"):
        source = path.read_text(encoding="utf-8")
        assert not re.search(r"\.(?:parallelStream|stream)\s*\(", source), f"Stream in {path}"
        lines = source.splitlines()
        for index, line in enumerate(lines):
            declaration = re.search(r"\b(public|private|protected)\b.*\(", line)
            class_line = re.search(r"\b(class|record)\s+\w+", line)
            if (declaration and "=" not in line) or class_line:
                previous = "\n".join(lines[max(0, index - 6):index])
                assert "/**" in previous, f"Kommentar fehlt: {path}:{index + 1}"
        checked += 1
    return {"java_files_checked": checked, "tracked_env": False,
            "note": "Kommentare zusaetzlich manuell auf Erklaerbarkeit pruefen"}


def main():
    """Fuehrt ein Szenario oder die geforderte Reihenfolge aus und sichert echte Messwerte."""
    global PROJECT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["all", "S2", "S3", "S4", "S5", "S6", "S7", "S8"])
    parser.add_argument("--project", default=os.environ.get("COMPOSE_PROJECT_NAME"))
    parser.add_argument("--report", default="tmp/acceptance-result.json")
    args = parser.parse_args()
    PROJECT = args.project
    scenarios = {"S2": s2, "S3": s3, "S4": s4, "S5": s5, "S6": s6, "S7": s7, "S8": s8}
    selected = scenarios
    if args.scenario != "all":
        selected = {args.scenario: scenarios[args.scenario]}
    results = {}
    for name, test in selected.items():
        print(f"{name} wird geprueft ...", flush=True)
        results[name] = test()
        print(json.dumps({name: results[name]}, ensure_ascii=False), flush=True)
    report = ROOT / args.report
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
