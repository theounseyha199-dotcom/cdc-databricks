"""OBS v5 RPC and a private OBS instance for the demonstration."""
import base64
import hashlib
import json
import os
import secrets
import selectors
import subprocess
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCENES = ("Introduction", "Architecture", "Terminal", "Source Code", "Kafbat UI",
          "Databricks", "Conclusion")


class OBS:
    def __init__(self, port, password):
        self.transport = subprocess.Popen(
            ["node", str(HERE / "ws_bridge.mjs"), f"ws://127.0.0.1:{port}"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1,
        )
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.transport.stdout, selectors.EVENT_READ)
        try:
            hello = self.receive()
            if hello["op"] != 0:
                raise RuntimeError("Unexpected OBS handshake")
            identification = {"rpcVersion": 1, "eventSubscriptions": 0}
            if auth := hello["d"].get("authentication"):
                digest = lambda value: base64.b64encode(hashlib.sha256(value.encode()).digest()).decode()
                identification["authentication"] = digest(
                    digest(password + auth["salt"]) + auth["challenge"]
                )
            self.send(1, identification)
            if self.receive()["op"] != 2:
                raise RuntimeError("OBS authentication failed")
        except BaseException:
            self.close()
            raise

    def receive(self):
        if not self.selector.select(10):
            raise TimeoutError("OBS did not respond within ten seconds")
        line = self.transport.stdout.readline()
        if not line:
            raise RuntimeError("OBS WebSocket closed the connection")
        message = json.loads(line)
        if "transportError" in message:
            raise ConnectionError(message["transportError"])
        return message

    def send(self, opcode, data):
        self.transport.stdin.write(json.dumps({"op": opcode, "d": data}) + "\n")
        self.transport.stdin.flush()

    def call(self, kind, **data):
        request_id = uuid.uuid4().hex
        self.send(6, {"requestType": kind, "requestId": request_id, "requestData": data})
        while True:
            response = self.receive()
            if response["op"] == 7 and response["d"]["requestId"] == request_id:
                response = response["d"]
                status = response["requestStatus"]
                if not status["result"]:
                    raise RuntimeError(f"OBS {kind} returned {status['code']}: {status.get('comment', '')}")
                return response.get("responseData", {})

    def status(self):
        return self.call("GetRecordStatus")

    def start(self):
        if self.status()["outputActive"]:
            raise RuntimeError("An OBS recording is already active")
        self.call("StartRecord")
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            status = self.status()
            if status["outputActive"] and status["outputDuration"] > 0:
                return
            time.sleep(0.2)
        raise RuntimeError("OBS accepted StartRecord but its recording did not start")

    def switch(self, scene):
        self.call("SetCurrentProgramScene", sceneName=scene)

    def pause(self):
        self.call("PauseRecord")

    def resume(self):
        self.call("ResumeRecord")

    def stop(self):
        return Path(self.call("StopRecord")["outputPath"])

    def close(self):
        self.selector.close()
        self.transport.terminate()
        self.transport.wait(timeout=5)

    def prepare(self, url, kafbat):
        if self.status()["outputActive"]:
            raise RuntimeError("OBS must be idle before preparing scenes")
        sizes = self.call("GetVideoSettings")
        actual = tuple(sizes[key] for key in
                       ("baseWidth", "baseHeight", "outputWidth", "outputHeight",
                        "fpsNumerator", "fpsDenominator"))
        if actual != (1920, 1080, 1920, 1080, 30, 1):
            raise RuntimeError(f"OBS video settings must be 1080p30 (actual {actual})")
        for input_name in self.call("GetSpecialInputs").values():
            if input_name:
                self.call("SetInputMute", inputName=input_name, inputMuted=True)
        names = {scene["sceneName"] for scene in self.call("GetSceneList")["scenes"]}
        inputs = {entry["inputName"] for entry in self.call("GetInputList")["inputs"]}
        for index, scene in enumerate(SCENES, 1):
            name = f"Flow 1 · {scene}"
            if name not in names:
                self.call("CreateScene", sceneName=name)
            source = f"Flow1 Browser {index}"
            settings = {"url": f"{url}/?scene={index}", "width": 1920,
                        "height": 1080, "is_local_file": False, "shutdown": False,
                        "restart_when_active": False}
            if source in inputs:
                self.call("SetInputSettings", inputName=source, inputSettings=settings)
            else:
                self.call("CreateInput", sceneName=name, inputName=source,
                          inputKind="browser_source", inputSettings=settings)
        name = "Flow 1 · Kafbat UI"
        source = "Flow1 Kafbat Topic"
        settings = {"url": kafbat, "width": 1712, "height": 600,
                    "is_local_file": False, "shutdown": False}
        if source in inputs:
            self.call("SetInputSettings", inputName=source, inputSettings=settings)
            item = self.call("GetSceneItemId", sceneName=name, sourceName=source)
        else:
            item = self.call("CreateInput", sceneName=name, inputName=source,
                             inputKind="browser_source", inputSettings=settings)
        self.call("SetSceneItemTransform", sceneName=name, sceneItemId=item["sceneItemId"],
                  sceneItemTransform={"positionX": 104, "positionY": 285})
        if "Fade" in {entry["transitionName"] for entry in self.call("GetSceneTransitionList")["transitions"]}:
            self.call("SetCurrentSceneTransition", transitionName="Fade")
            self.call("SetCurrentSceneTransitionDuration", transitionDuration=400)


def launch(directory, port):
    config = directory / "obs-studio"
    profile = config / "basic/profiles/Flow1"
    profile.mkdir(parents=True)
    (config / "user.ini").write_text("""[General]
FirstRun=true
ConfirmOnExit=false

[Basic]
Profile=Flow1
ProfileDir=Flow1
ConfigOnNewProfile=false

[BasicWindow]
WarnBeforeStartingRecord=false
WarnBeforeStoppingRecord=false
""")
    output = HERE / "output"
    (profile / "basic.ini").write_text(f"""[General]
Name=Flow1

[Video]
BaseCX=1920
BaseCY=1080
OutputCX=1920
OutputCY=1080
FPSType=0
FPSCommon=30

[Output]
Mode=Simple
FilenameFormatting=Flow1_%CCYY-%MM-%DD_%hh-%mm-%ss

[SimpleOutput]
FilePath={output}
RecFormat2=mkv
RecQuality=HQ
RecEncoder=x264
RecRB=false
""")
    socket = config / "plugin_config/obs-websocket"
    socket.mkdir(parents=True)
    password = secrets.token_urlsafe(28)
    auth_file = socket / "config.json"
    with open(auth_file, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as stream:
        json.dump({"server_enabled": True, "server_port": port, "auth_required": True,
                   "server_password": password, "alerts_enabled": False,
                   "first_load": False}, stream)
    process = subprocess.Popen(
        ["obs", "--multi", "--profile", "Flow1", "--disable-updater", "--minimize-to-tray"],
        env={**os.environ, "XDG_CONFIG_HOME": str(directory)},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    controller = None
    for _ in range(35):
        try:
            if controller is None:
                controller = OBS(port, password)
            controller.status()
            return process, controller
        except (RuntimeError, TimeoutError, ConnectionError) as error:
            if controller is not None and "returned 207" not in str(error):
                controller.close()
                controller = None
            if process.poll() is not None:
                raise RuntimeError(f"Isolated OBS exited with status {process.returncode}")
            time.sleep(0.8)
    if controller is not None:
        controller.close()
    process.terminate()
    raise RuntimeError("Isolated OBS did not open its WebSocket")
