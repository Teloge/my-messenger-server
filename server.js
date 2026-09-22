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
const GROUPS_FILE = path.join(DATA_DIR, "groups.json");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);
if (!fs.existsSync(DB_FILE)) fs.writeFileSync(DB_FILE, "[]");
if (!fs.existsSync(USERS_FILE)) fs.writeFileSync(USERS_FILE, "[]");
if (!fs.existsSync(GROUPS_FILE)) fs.writeFileSync(GROUPS_FILE, "[]");

function readJSON(f, def) { try { return JSON.parse(fs.readFileSync(f, "utf8")); } catch { return def; } }
function writeJSON(f, d) { fs.writeFileSync(f, JSON.stringify(d, null, 2)); }

app.use("/files", express.static(UPLOAD_DIR));
app.use(express.static(path.join(__dirname, "public")));
app.get("/", (req, res) => res.send("Messenger server v6 running"));

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
    socket.join(socket.id);
    if (record.username) saveUser({ ...record, id: record.username });
    io.emit("users", Array.from(online.values()).map(publicUser));

    const groups = readJSON(GROUPS_FILE, []);
    const mine = groups.filter(g => g.members.includes(socket.id));
    socket.emit("my-groups", mine);
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
    } catch (e) { if (typeof ack === "function") ack({ ok: false, error: e.message }); }
  });

  socket.on("message", (msg) => {
    const u = online.get(socket.id) || { name: "Аноним" };
    const payload = {
      id: "m_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6),
      from: socket.id, to: msg.to || null,
      name: u.name, username: u.username,
      text: msg.text || "",
      fileUrl: msg.fileUrl || null, fileType: msg.fileType || null,
      duration: msg.duration || null,
      room: msg.room || "global",
      reply: msg.reply || null, forwarded: msg.forwarded || false,
      status: "sent", ts: Date.now()
    };
    const db = readJSON(DB_FILE, []);
    db.push(payload);
    if (db.length > 10000) db.splice(0, db.length - 10000);
    writeJSON(DB_FILE, db);
    socket.emit("message-ack", { tempId: msg.tempId, realId: payload.id, ts: payload.ts, status: "sent" });
    socket.broadcast.emit("message", payload);
    const recipient = Array.from(online.values()).find(x => x.id === msg.to);
    if (recipient) socket.emit("message-status", { messageIds: [payload.id], status: "delivered" });
  });

  socket.on("create-group", (data, ack) => {
    const groups = readJSON(GROUPS_FILE, []);
    const g = {
      id: "g_" + Date.now().toString(36),
      name: data.name || "Группа",
      type: data.type || "group",
      owner: socket.id,
      ownerName: (online.get(socket.id) || {}).name || "Админ",
      avatar: data.avatar || "",
      members: [socket.id, ...((data.members || []).filter(m => m !== socket.id))],
      createdAt: Date.now()
    };
    groups.push(g);
    writeJSON(GROUPS_FILE, groups);
    g.members.forEach(mid => io.to(mid).emit("group-created", g));
    if (typeof ack === "function") ack({ ok: true, group: g });
    console.log("group created:", g.id, g.name, "members:", g.members.length);
  });

  socket.on("group-message", (msg) => {
    const groups = readJSON(GROUPS_FILE, []);
    const g = groups.find(x => x.id === msg.groupId);
    if (!g) return;
    if (g.type === "channel" && g.owner !== socket.id) return;

    const u = online.get(socket.id) || { name: "Аноним" };
    const payload = {
      id: "m_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6),
      from: socket.id, name: u.name, username: u.username,
      text: msg.text || "",
      fileUrl: msg.fileUrl || null, fileType: msg.fileType || null,
      duration: msg.duration || null,
      room: g.id, groupId: g.id, isGroup: true,
      reply: msg.reply || null, forwarded: msg.forwarded || false,
      ts: Date.now(), status: "sent"
    };
    const db = readJSON(DB_FILE, []);
    db.push(payload);
    if (db.length > 10000) db.splice(0, db.length - 10000);
    writeJSON(DB_FILE, db);
    socket.emit("message-ack", { tempId: msg.tempId, realId: payload.id, ts: payload.ts, status: "sent" });
    g.members.forEach(mid => {
      if (mid === socket.id) return;
      io.to(mid).emit("group-message", payload);
    });
  });

  socket.on("update-group", (data) => {
    const groups = readJSON(GROUPS_FILE, []);
    const g = groups.find(x => x.id === data.id);
    if (!g || g.owner !== socket.id) return;
    if (data.name) g.name = data.name;
    if (data.avatar !== undefined) g.avatar = data.avatar;
    if (data.members) g.members = data.members;
    writeJSON(GROUPS_FILE, groups);
    g.members.forEach(mid => io.to(mid).emit("group-updated", g));
  });

  socket.on("leave-group", (data) => {
    const groups = readJSON(GROUPS_FILE, []);
    const g = groups.find(x => x.id === data.id);
    if (!g) return;
    g.members = g.members.filter(m => m !== socket.id);
    writeJSON(GROUPS_FILE, groups);
    g.members.forEach(mid => io.to(mid).emit("group-updated", g));
    socket.emit("group-left", { id: data.id });
  });

  socket.on("history-group", (data) => {
    const db = readJSON(DB_FILE, []);
    const filtered = db.filter(m => m.room === data.groupId).slice(-300);
    socket.emit("history-group", { groupId: data.groupId, messages: filtered });
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

  // ---------- WebRTC сигналинг ----------
  socket.on("call-offer", (data) => {
    const from = online.get(socket.id);
    if (!from) return;
    console.log("call-offer:", from.name, "->", data.to);
    io.to(data.to).emit("call-incoming", {
      from: socket.id,
      fromName: from.name,
      fromAvatar: from.avatar || "",
      sdp: data.sdp,
      callType: data.callType || "audio"
    });
  });

  socket.on("call-answer", (data) => {
    io.to(data.to).emit("call-answered", { from: socket.id, sdp: data.sdp });
  });

  socket.on("call-ice", (data) => {
    io.to(data.to).emit("call-ice", { from: socket.id, candidate: data.candidate });
  });

  socket.on("call-reject", (data) => {
    io.to(data.to).emit("call-rejected", { from: socket.id });
  });

  socket.on("call-end", (data) => {
    io.to(data.to).emit("call-ended", { from: socket.id });
  });

  socket.on("disconnect", () => {
    online.delete(socket.id);
    io.emit("users", Array.from(online.values()).map(publicUser));
  });
});

server.listen(PORT, () => console.log("Server v6 started on port " + PORT));
