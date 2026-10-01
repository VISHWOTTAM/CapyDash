// CapyDash relay server: room codes + message forwarding over WebSockets.
// The host phone runs the game; this server only passes messages between players.
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;
const rooms = new Map(); // code -> { code, peers: Map<id, ws>, nextId, started }

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`CapyDash relay OK - ${rooms.size} room(s)\n`);
});
const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });

function newCode() {
  const A = "ABCDEFGHJKLMNPQRSTUVWXYZ";
  let c;
  do {
    c = "";
    for (let i = 0; i < 4; i++) c += A[Math.floor(Math.random() * A.length)];
  } while (rooms.has(c));
  return c;
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.t === "create") {
      if (ws.room) return;
      const code = newCode();
      const room = { code, peers: new Map([[1, ws]]), nextId: 2, started: false };
      rooms.set(code, room);
      ws.room = room;
      ws.pid = 1;
      send(ws, { t: "created", code, id: 1 });
    } else if (m.t === "join") {
      if (ws.room) return;
      const room = rooms.get(String(m.code || "").trim().toUpperCase());
      if (!room) return send(ws, { t: "error", msg: "Room not found" });
      if (room.peers.size >= MAX_PLAYERS) return send(ws, { t: "error", msg: "Room is full" });
      if (room.started) return send(ws, { t: "error", msg: "Game already started" });
      const id = room.nextId++;
      room.peers.set(id, ws);
      ws.room = room;
      ws.pid = id;
      send(ws, { t: "joined", code: room.code, id, peers: [...room.peers.keys()] });
      for (const [pid, p] of room.peers) if (pid !== id) send(p, { t: "peer_join", id });
    } else if (m.t === "msg" && ws.room) {
      const room = ws.room;
      if (ws.pid === 1 && typeof m.started === "boolean") room.started = m.started;
      const out = JSON.stringify({ t: "msg", from: ws.pid, d: m.d });
      if (m.to) {
        const p = room.peers.get(m.to);
        if (p && p.readyState === 1) p.send(out);
      } else {
        for (const [pid, p] of room.peers) if (pid !== ws.pid && p.readyState === 1) p.send(out);
      }
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    if (!room) return;
    room.peers.delete(ws.pid);
    if (ws.pid === 1) {
      for (const p of room.peers.values()) { send(p, { t: "room_closed" }); p.room = null; }
      rooms.delete(room.code);
    } else {
      for (const p of room.peers.values()) send(p, { t: "peer_leave", id: ws.pid });
    }
  });
});

// drop dead connections (phones that lost signal)
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);

server.listen(PORT, () => console.log(`CapyDash relay listening on ${PORT}`));
