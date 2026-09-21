const express = require("express");
const http = require("http");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
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

function readJSON(file, def) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); }
  catch { return def; }
}
function writeJSON(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || "";
    cb(null, Date.now() + "_" + Math.random().toString(36).slice(2, 8) + ext);
  }
});
const upload = multer({ storage, limits: { fileSize: 100 * 1024 * 1024 } });

app.use("/files", express.static(UPLOAD_DIR));
app.use(express.static(path.join(__dirname, "public")));

app.post("/upload", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "no file" });
  const url = "/files/" + req.file.filename;
  res.json({
    url,
    size: req.file.size,
    name: req.file.originalname,
    mime: req.file.mimetype
  });
});

app.get("/", (req, res) => res.send("Messenger server v2 running"));

const online = new Map();

function saveUser(u) {
  const users = readJSON(USERS_FILE, []);
  const idx = users.findIndex(x => x.username === u.username);
  if (idx === -1) users.push(u);
  else users[idx] = u;
  writeJSON(USERS_FILE, users);
}

function publicUser(u) {
  return { id: u.id, name: u.name, username: u.username,
           phone: u.phone, bio: u.bio, avatar: u.avatar || "", online: true };
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
      ts: Date.now()
    };
    const db = readJSON(DB_FILE, []);
    db.push(payload);
    if (db.length > 5000) db.splice(0, db.length - 5000);
    writeJSON(DB_FILE, db);
    io.emit("message", payload);
  });

  socket.on("history", (data) => {
    const db = readJSON(DB_FILE, []);
    const peer = data.peer;
    const myId = socket.id;
    const filtered = db.filter(m =>
      (m.from === myId && m.room === peer) ||
      (m.from === peer && m.room === myId) ||
      (m.room === peer && m.room === myId)
    ).slice(-200);
    socket.emit("history", filtered);
  });

  socket.on("typing", (data) => {
    const u = online.get(socket.id) || { name: "Аноним" };
    socket.broadcast.emit("typing", {
      from: socket.id, name: u.name, room: data.room || "global"
    });
  });

  socket.on("read", (data) => {
    socket.broadcast.emit("read", {
      from: socket.id, room: data.room, messageIds: data.messageIds || []
    });
  });

  socket.on("disconnect", () => {
    online.delete(socket.id);
    io.emit("users", Array.from(online.values()).map(publicUser));
    console.log("disconnect:", socket.id);
  });
});

server.listen(PORT, () => console.log("Server v2 started on port " + PORT));
