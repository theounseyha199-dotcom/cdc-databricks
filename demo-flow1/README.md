# PostgreSQL to Data Lakehouse

Flow 1 — Real-Time CDC Pipeline Using Python

The silent OBS walkthrough displays the live Compose and connector status, the Kafbat topic overview, retained Kafka events, the existing Python implementation, and read-only Databricks SQL results. Khmer captions are rendered in the scene. The accompanying SRT uses OBS recording timestamps.

## Run on the Arch Linux desktop

Run from a desktop terminal with access to Docker, Wayland, and the project virtual environment.

```sh
./demo-flow1/run_demo.sh --dry-run
./demo-flow1/run_demo.sh --record-read-only
```

The Databricks connector opens the existing browser OAuth flow before OBS starts. The dry run checks all data sources, creates the OBS scenes, and saves preview screenshots. A project-local OBS profile uses 1920×1080 at 30 FPS and saves the original to MKV. Capture uses OBS Browser Sources for the presentation and the topic overview. It needs no desktop capture grant.

The read-only recording leaves the existing consumer running. It demonstrates INSERT, UPDATE, DELETE, and snapshot history from retained CDC events. The consumer recovery segment shows the live committed resume position. A live mutation and restart demonstration requires a PostgreSQL write login and explicit approval to stop the running consumer.

The recorder uses the project's Python environment and Node's built-in WebSocket. No package installation is required. FFmpeg remuxes just the video stream to MP4, and FFprobe verifies the specs.

## Files

- `output/Flow1_Demo.mp4` is the silent delivery file.
- `output/Flow1_*.mkv` is the preserved OBS original.
- `output/Flow1_Demo_Khmer.srt` contains timestamped Khmer captions.
- `output/demo_report.md` records the verified scope and results.
- `screenshots/` contains the dry-run previews.
- `logs/` is used when invoking `run_demo.sh --background --record-read-only`.

The recorded SQL results come from the Databricks SQL connector. The captions and sources use the sanitized live evidence, and the Kafka overview stays on the topic overview page. Existing source payloads and credential files are excluded from the recording.

[OBS WebSocket 5 protocol](https://github.com/obsproject/obs-websocket/blob/master/docs/generated/protocol.md)
