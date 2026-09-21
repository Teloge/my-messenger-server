const { io } = require("socket.io-client");

const socket = io("https://my-messenger-server.relaxdev.ru", {
  transports: ["websocket"]
});

socket.on("connect", () => {
  console.log("[OK] подключился к серверу, id:", socket.id);
  socket.emit("join", { name: "TestUser" });
  setTimeout(() => {
    console.log("[..] отправляю тестовое сообщение");
    socket.emit("message", { text: "hello from test", room: "test" });
  }, 500);
});

socket.on("message", (msg) => {
  console.log("[OK] получил сообщение от сервера:");
  console.log("     от:", msg.name, "| текст:", msg.text);
});

socket.on("connect_error", (err) => {
  console.log("[FAIL] ошибка подключения:", err.message);
});

setTimeout(() => {
  console.log("[..] тест завершён");
  process.exit(0);
}, 6000);
