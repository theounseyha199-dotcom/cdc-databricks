import { createInterface } from "node:readline";

const socket = new WebSocket(process.argv[2], "obswebsocket.json");
const input = createInterface({input: process.stdin});
socket.addEventListener("message", ({data}) => process.stdout.write(data + "\n"));
socket.addEventListener("error", () => {
  process.stdout.write(JSON.stringify({ transportError: "OBS WebSocket connection failed" }) + "\n");
});
socket.addEventListener("close", () => process.exit(0));
input.on("line", line => socket.send(line));
input.on("close", () => {
  socket.close();
  process.exit(0);
});
