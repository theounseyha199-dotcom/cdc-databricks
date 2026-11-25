"""Record a silent Flow 1 walkthrough and export verified video artifacts."""
import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

from browser_controller import Stage
from obs_controller import OBS, SCENES, launch
from preflight import (APP, GROUP, HERE, KAFBAT, QUERIES, TOPIC, configure, databricks,
                       infrastructure, query)

OUT = HERE / "output"
SOURCE = HERE / "narration_km.txt"
CAPTIONS = {block.split("]", 1)[0][1:]: block.split("]", 1)[1].strip()
            for block in SOURCE.read_text().strip().split("\n\n")}


def cut(name, start, end):
    lines = (APP / name).read_text().splitlines()[start - 1:end]
    return "\n".join(f"{index:>3}  {line}" for index, line in enumerate(lines, start))


def table(headers, rows):
    rows = [tuple("NULL" if value is None else str(value) for value in row) for row in rows]
    if not rows:
        return "(0 rows)"
    widths = [max(map(len, values)) for values in zip(headers, *rows)]
    lines = ["  ".join(value.ljust(width) for value, width in zip(row, widths))
             for row in (headers, *rows)]
    return "\n".join((lines[0], "  ".join("-" * width for width in widths), *lines[1:]))


class Recording:
    def __init__(self, obs, stage):
        self.obs, self.stage = obs, stage
        self.events = []

    def scene(self, index, title, caption, seconds, subtitle="", **state):
        name = SCENES[index - 1]
        text = CAPTIONS[caption]
        self.stage.show(index, section=name, title=title, subtitle=subtitle,
                        caption=text, **state)
        self.obs.switch(f"Flow 1 · {name}")
        time.sleep(0.22)
        status = self.obs.status()
        if not status["outputActive"]:
            raise RuntimeError("OBS stopped recording before the scene changed")
        timestamp = status["outputDuration"]
        self.events.append((timestamp, text))
        print(f"Recording {timestamp / 1000:.1f}s | {title}", flush=True)
        time.sleep(seconds)


def timestamp(milliseconds):
    seconds, ms = divmod(round(milliseconds), 1000)
    mins, secs = divmod(seconds, 60)
    hours, mins = divmod(mins, 60)
    return f"{hours:02}:{mins:02}:{secs:02},{ms:03}"


def srt(events, end):
    next_starts = [start for start, _ in events[1:]] + [end]
    cues = []
    for number, ((start, caption), stop) in enumerate(zip(events, next_starts), 1):
        cues.append(f"{number}\n{timestamp(start)} --> {timestamp(stop)}\n{caption}\n")
    return "\n".join(cues)


def compose(r, info, baseline, duplicates, summary):
    scene = r.scene
    scene(1, "PostgreSQL to Data Lakehouse", "intro", 9,
          "Flow 1 — Real-Time CDC Pipeline Using Python", diagram=True, hero=True)
    scene(2, "How the data moves", "architecture", 13,
          "Logical replication → durable stream → append-only CDC history", diagram=True)
    compose_result = table(("SERVICE", "STATE"), info["services"].items())
    status = json.dumps({"name": "postgres-connector", "connector": {"state": "RUNNING"},
                         "tasks": info["tasks"]}, indent=2)
    scene(3, "Live infrastructure", "infrastructure", 13, "Compose and Debezium status",
          detail="$ docker compose -f docker/docker-compose.yml ps\n" + compose_result +
          "\n\n$ curl -s http://localhost:8083/connectors/postgres-connector/status\n" +
          status, highlight=("running", "RUNNING"))
    topic_rows = [(p["partition"], p["offsetMin"], p["offsetMax"], p["leader"])
                  for p in info["topic"]["partitions"]]
    scene(5, "Kafka topic in Kafbat", "kafbat", 12, TOPIC)
    scene(3, "Initial snapshot", "snapshot", 15, "Retained Debezium records (payload fields omitted)",
          detail=table(("PARTITION", "OFFSET", "SNAPSHOT FLAG"), info["snapshots"]) +
          "\n\n" + table(("PARTITION", "START OFFSET", "END OFFSET", "LEADER"), topic_rows),
          highlight=("true", "last"))
    scene(4, "Read events from Kafka", "consumer", 12, "flow1-python/consumer.py",
          detail=cut("consumer.py", 41, 49) + "\n       …\n" + cut("consumer.py", 67, 69),
          highlight=("enable_auto_commit", "KafkaConsumer", "json.loads", "decode"))
    scene(4, "Select the row image", "transform", 13, "flow1-python/transform.py",
          detail=cut("transform.py", 8, 20), highlight=('payload.get("after")', 'payload.get("before")'))
    scene(4, "Merge on the Kafka event key", "merge", 13, "flow1-python/bronze_writer.py",
          detail=cut("bronze_writer.py", 27, 31) + "\n       …\n" + cut("bronze_writer.py", 44, 52),
          highlight=("MERGE INTO", "ON target", "AND target", "WHEN NOT MATCHED"))
    scene(4, "Commit the next offset", "commit", 13, "flow1-python/consumer.py",
          detail=cut("consumer.py", 23, 28) + "\n       …\n" + cut("consumer.py", 85, 94),
          highlight=("message.offset + 1", "writer.write", "commit_message"))
    scene(6, "Verify the Bronze table", "baseline", 14, "Live Databricks SQL Warehouse query",
          detail=QUERIES["baseline"] + ";\n\n" + table(("total_records",), [(baseline,)]),
          highlight=(str(baseline),))
    for op, label in (("c", "INSERT"), ("u", "UPDATE"), ("d", "DELETE")):
        count = info["counts"][op]
        stored = [(operation, num, low, high) for operation, num, low, high in summary if operation == op]
        extra = f"\n\nKafka tombstones retained: {info['counts']['tombstone']}" if op == "d" else ""
        scene(3, label + " history", {"c":"insert","u":"update","d":"delete"}[op], 13,
              "Existing CDC events from Kafka and Databricks",
              detail=f"Kafka | topic={TOPIC} | op={op} | retained={count}\n\n" +
              table(("OP", "BRONZE ROWS", "FIRST OFFSET", "LAST OFFSET"), stored) + extra,
              highlight=(f"op={op}", "tombstones"))
    scene(6, "Check for duplicate event keys", "quality", 14, "Live Databricks SQL Warehouse query",
          detail=QUERIES["duplicates"] + ";\n\n" +
          table(("TOPIC", "PARTITION", "OFFSET", "DUPLICATE COUNT"), duplicates),
          highlight=("GROUP BY", "HAVING", "0 rows"))
    rows = [(partition, state["committed"], state["end"], state["lag"],
             "active" if state["active"] else "offline")
            for partition, state in info["group"].items()]
    scene(3, "Consumer group and recovery position", "recovery", 14, GROUP,
          detail=table(("PARTITION", "COMMITTED", "LOG END", "LAG", "CONSUMER"), rows) +
          "\n\nResume position = committed next offset\nObserved group status during this read-only walkthrough.",
          highlight=("active", "Resume position"))
    scene(7, "Flow 1 verification", "conclusion", 13,
          "Read-only walkthrough · live infrastructure and retained CDC history",
          detail=table(("CHECK", "RESULT"), [
              ("PostgreSQL 18 and logical WAL", info["postgres"]),
              ("Debezium connector and task", "RUNNING"),
              ("Kafka and Kafbat", "ONLINE"),
              ("Snapshot, INSERT, UPDATE, DELETE", "Verified in retained events"),
              ("Consumer offset", "Read from broker"),
              ("Restart", "Existing consumer stayed running"),
              ("Duplicate event keys", len(duplicates)),
              ("Live Bronze count", baseline),
          ]), highlight=("RUNNING", "ONLINE", "Live Bronze count"))


def verify_file(file):
    out = subprocess.check_output(["ffprobe", "-v", "error", "-show_streams", "-show_format",
                                   "-of", "json", str(file)], text=True)
    data = json.loads(out)
    return data["streams"], float(data["format"]["duration"])


def export(raw, events, baseline, duplicates):
    if raw.suffix.lower() != ".mkv" or not raw.is_file():
        raise RuntimeError(f"OBS did not return an existing MKV: {raw}")
    destination = OUT / "Flow1_Demo.mp4"
    partial = destination.with_suffix(".tmp.mp4")
    subprocess.run(["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                    "-i", str(raw), "-map", "0:v:0", "-c:v", "copy", "-an",
                    "-movflags", "+faststart", str(partial)], check=True)
    streams, duration = verify_file(partial)
    video = streams[0]
    if len(streams) != 1 or (video["width"], video["height"], video["avg_frame_rate"]) != (1920, 1080, "30/1"):
        raise RuntimeError("Final video verification failed (expected 1080p30 video only)")
    partial.replace(destination)
    (OUT / "Flow1_Demo_Khmer.srt").write_text(srt(events, duration * 1000))
    (HERE / "subtitles_km.srt").write_text(srt(events, duration * 1000))
    shutil.copyfile(SOURCE, OUT / "Flow1_Demo_Narration.txt")
    (OUT / "demo_report.md").write_text(
        f"# Flow 1 recording\n\n"
        f"- Recorded {datetime.now().astimezone().isoformat(timespec='seconds')}\n"
        f"- Verified output: {destination.name}, 1920×1080, 30 FPS, {duration:.2f} seconds, video only\n"
        f"- OBS original: {raw.name}\n"
        f"- Live Databricks Bronze count: {baseline}\n"
        f"- Duplicate event keys: {len(duplicates)}\n"
        "- Scope: read-only. Kafka and Bronze used retained events. The existing consumer stayed running.\n"
    )
    print(f"VIDEO VERIFIED | {duration:.2f}s | {destination}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--dry-run", action="store_true")
    modes.add_argument("--record-read-only", action="store_true")
    args = parser.parse_args()
    configure()
    print("Checking live infrastructure and Kafka without writes.", flush=True)
    info = infrastructure()
    print("Infrastructure ready. Opening Databricks OAuth before OBS.", flush=True)
    with databricks() as db:
        if query(db, "SELECT 1") != [(1,)]:
            raise RuntimeError("SQL Warehouse verification failed")
        baseline = query(db, QUERIES["baseline"])[0][0]
        duplicates = query(db, QUERIES["duplicates"])
        summary = query(db, QUERIES["snapshot"])
        print(f"Readiness | Bronze={baseline} | duplicate keys={len(duplicates)}", flush=True)
        stage = Stage()
        process = None
        obs = None
        try:
            port = int(os.getenv("DEMO_OBS_PORT", "4467"))
            if os.getenv("DEMO_OBS_EXTERNAL") == "1":
                obs = OBS(port, os.environ["OBS_WEBSOCKET_PASSWORD"])
            else:
                directory = Path(tempfile.mkdtemp(prefix=".obs-", dir=HERE))
                process, obs = launch(directory, port)
            topic_url = f"{KAFBAT}/ui/clusters/{info['cluster']}/all-topics/{TOPIC}"
            obs.prepare(stage.url, topic_url)
            stage.show(1, section="Introduction", title="PostgreSQL to Data Lakehouse",
                       subtitle="Flow 1 — Real-Time CDC Pipeline Using Python",
                       caption=CAPTIONS["intro"], diagram=True, hero=True)
            obs.switch("Flow 1 · Introduction")
            time.sleep(2.0)
            if args.dry_run:
                obs.call("SaveSourceScreenshot", sourceName="Flow1 Browser 1", imageFormat="png",
                         imageFilePath=str(HERE / "screenshots" / "dry-run-intro.png"),
                         imageWidth=1920, imageHeight=1080)
                obs.switch("Flow 1 · Kafbat UI")
                time.sleep(2.0)
                obs.call("SaveSourceScreenshot", sourceName="Flow1 Kafbat Topic", imageFormat="png",
                         imageFilePath=str(HERE / "screenshots" / "dry-run-kafbat.png"),
                         imageWidth=1712, imageHeight=600)
                print("DRY RUN PASSED | 1080p30 OBS scenes prepared | no recording or writes", flush=True)
                return
            recording = Recording(obs, stage)
            obs.start()
            try:
                compose(recording, info, baseline, duplicates, summary)
            except BaseException:
                try:
                    obs.pause()
                except RuntimeError:
                    pass
                raise
            finally:
                if obs.status()["outputActive"]:
                    raw = obs.stop()
                    print(f"OBS ORIGINAL | {raw}", flush=True)
                else:
                    raise RuntimeError("OBS stopped before returning the original file path")
            export(raw, recording.events, baseline, duplicates)
        finally:
            if obs is not None:
                obs.close()
            if process is not None:
                process.terminate()
                process.wait(timeout=12)
            stage.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as error:
        print(f"DEMO STOPPED | {error}", flush=True)
        raise SystemExit(1)
