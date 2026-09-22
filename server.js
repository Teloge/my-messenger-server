const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ["websocket", "polling"],
  maxHttpBufferSize: 1e8
});

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, "data");
const UPLOAD_DIR = path.join(DATA_DIR, "uploads");
const DB_FILE = path.join(DATA_DIR, "messages.json");
const USERS_FILE = path.join(DATA_DIR, "users.json");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);
if (!fs.existsSync(DB_FILE)) fs.writeFileSync(DB_FILE, "[]");
if (!fs.existsSync(USERS_FILE)) fs.writeFileSync(USERS_FILE, "[]");

function readJSON(f, def) { try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch { return def; } }
function writeJSON(f, d) { fs.writeFileSync(f, JSON.stringify(d, null, 2)); }

app.use("/files", express.static(UPLOAD_DIR));
app.use(express.static(path.join(__dirname, "public")));
app.get("/", (req, res) => res.send("Messenger server v4 running"));

const online = new Map();

function saveUser(u) {
  const users = readJSON(USERS_FILE, []);
  const idx = users.findIndex(x => x.username === u.username);
  if (idx === -1) users.push(u); else users[idx] = u;
  writeJSON(USERS_FILE, users);
}
function publicUser(u) {
  return { id: u.id, name: u.name, username: u.username, phone: u.phone,
           bio: u.bio, avatar: u.avatar || "", online: true };
}

io.on("connection", (socket) => {
  console.log("connect:", socket.id);

  socket.on("join", (user) => {
    const record = {
      id: socket.id,
      name: user.name || "Аноним",
      username: user.username || "",
      phone: user.phone || "",
      bio: user.bio || "",
      avatar: user.avatar || ""
    };
    online.set(socket.id, record);
    if (record.username) saveUser({ ...record, id: record.username });
    io.emit("users", Array.from(online.values()).map(publicUser));
  });

  socket.on("upload", (data, ack) => {
    try {
      const base64 = data.data.includes(",") ? data.data.split(",")[1] : data.data;
      const buf = Buffer.from(base64, "base64");
      const ext = path.extname(data.name) || "";
      const filename = Date.now() + "_" + Math.random().toString(36).slice(2, 8) + ext;
      fs.writeFileSync(path.join(UPLOAD_DIR, filename), buf);
      const url = "/files/" + filename;
      if (typeof ack === "function") ack({ ok: true, url, size: buf.length, name: data.name });
    } catch (e) {
      if (typeof ack === "function") ack({ ok: false, error: e.message });
    }
  });

  socket.on("message", (msg) => {
    const u = online.get(socket.id) || { name: "Аноним" };
    const payload = {
      id: "m_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6),
      from: socket.id,
      to: msg.to || null,
      name: u.name,
      username: u.username,
      text: msg.text || "",
      fileUrl: msg.fileUrl || null,
      fileType: msg.fileType || null,
      duration: msg.duration || null,
      room: msg.room || "global",
      reply: msg.reply || null,
      forwarded: msg.forwarded || false,
      status: "sent",
      ts: Date.now()
    };
    const db = readJSON(DB_FILE, []);
    db.push(payload);
    if (db.length > 10000) db.splice(0, db.length - 10000);
    writeJSON(DB_FILE, db);

    // отправили себе — подтверждение
    socket.emit("message-ack", { tempId: msg.tempId, realId: payload.id, ts: payload.ts, status: "sent" });

    // остальным
    socket.broadcast.emit("message", payload);

    // пометили как доставленное — если получатель онлайн
    const recipient = Array.from(online.values()).find(x => x.id === msg.to);
    if (recipient) {
      socket.emit("message-status", { messageIds: [payload.id], status: "delivered" });
      payload.status = "delivered";
    }
  });

  socket.on("edit", (data) => {
    const db = readJSON(DB_FILE, []);
    const idx = db.findIndex(m => m.id === data.id && m.from === socket.id);
    if (idx < 0) return;
    db[idx].text = data.text;
    db[idx].edited = true;
    writeJSON(DB_FILE, db);
    io.emit("message-edited", { id: data.id, text: data.text, edited: true });
  });

  socket.on("delete", (data) => {
    const db = readJSON(DB_FILE, []);
    const idx = db.findIndex(m => m.id === data.id && m.from === socket.id);
    if (idx < 0) return;
    db.splice(idx, 1);
    writeJSON(DB_FILE, db);
    io.emit("message-deleted", { id: data.id });
  });

  socket.on("history", (data) => {
    const db = readJSON(DB_FILE, []);
    const myId = socket.id;
    const peer = data.peer;
    const filtered = db.filter(m =>
      (m.from === myId && m.room === peer) ||
      (m.from === peer && m.room === myId)
    ).slice(-300);
    socket.emit("history", filtered);
  });

  socket.on("mark-read", (data) => {
    const db = readJSON(DB_FILE, []);
    let changed = [];
    db.forEach(m => {
      if (m.room === socket.id && m.from === data.peer && m.status !== "read") {
        m.status = "read"; changed.push(m.id);
      }
    });
    if (changed.length) writeJSON(DB_FILE, db);
    if (changed.length) socket.broadcast.emit("message-status", { from: socket.id, messageIds: changed, status: "read" });
  });

  socket.on("typing", (data) => {
    const u = online.get(socket.id) || { name: "Аноним" };
    socket.broadcast.emit("typing", { from: socket.id, name: u.name, room: data.room || "global" });
  });

  socket.on("disconnect", () => {
    online.delete(socket.id);
    io.emit("users", Array.from(online.values()).map(publicUser));
    console.log("disconnect:", socket.id);
  });
});

server.listen(PORT, () => console.log("Server v4 started on port " + PORT));
