const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ["websocket", "polling"]
});

const PORT = process.env.PORT || 3000;

const online = new Map();

io.on("connection", (socket) => {
  console.log("подключился:", socket.id);

  socket.on("join", (user) => {
    socket.user = user;
    online.set(socket.id, { id: socket.id, name: user.name });
    io.emit("online", Array.from(online.values()));
    console.log("вошёл:", user.name, "| онлайн:", online.size);
  });

  socket.on("message", (msg) => {
    const payload = {
      id: "m_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6),
      from: socket.id,
      name: (socket.user && socket.user.name) || "Аноним",
      text: msg.text,
      room: msg.room || "global",
      reply: msg.reply || null,
      ts: Date.now()
    };
    io.emit("message", payload);
  });

  socket.on("typing", (data) => {
    socket.broadcast.emit("typing", {
      from: socket.id,
      name: (socket.user && socket.user.name) || "Аноним",
      room: data.room || "global"
    });
  });

  socket.on("disconnect", () => {
    online.delete(socket.id);
    io.emit("online", Array.from(online.values()));
    console.log("отключился:", socket.id);
  });
});

app.get("/", (req, res) => res.send("Messenger server is running!"));

server.listen(PORT, () => {
  console.log("Сервер запущен на порту " + PORT);
});
