const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const app = express();
app.use(express.json({ limit: "256kb" }));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

// Простое хранилище в памяти. При перезапуске сервера данные сбрасываются.
const users = new Map();      // login -> { login, phone, pub, c }
const phones = new Map();     // phone -> login
const sessions = new Map();   // token -> login
const chats = new Map();      // chatId -> [message]
const calls = [];             // история звонков
const online = new Map();     // login -> ws

const norm = s => String(s || "").trim();
const isLogin = s => /^[a-z0-9_]{3,20}$/.test(s);
const isPhone = s => /^\+?[\d\s()-]{7,}$/.test(s);
const chatId = (a, b) => [a, b].sort().join("_");
const token = () => crypto.randomBytes(24).toString("hex");

function auth(req, res, next) {
  const t = (req.headers.authorization || "").replace("Bearer ", "");
  const login = sessions.get(t);
  if (!login) return res.status(401).json({ error: "no session" });
  req.me = login;
  next();
}

app.get("/", (req, res) => res.send("SLIMOCnat server is running"));

// Регистрация: номер + логин + публичный ключ шифрования
app.post("/api/register", (req, res) => {
  const login = norm(req.body.login).toLowerCase().replace(/^@/, "");
  const phone = norm(req.body.phone);
  const pub = req.body.pub;
  if (!isLogin(login)) return res.status(400).json({ error: "Логин: 3–20 символов, латиница, цифры, _" });
  if (!isPhone(phone)) return res.status(400).json({ error: "Введи номер телефона" });
  if (users.has(login)) return res.status(409).json({ error: "Этот логин занят" });
  const user = { login, phone, pub, c: Math.floor(Math.random() * 6) };
  users.set(login, user);
  phones.set(phone, login);
  const t = token();
  sessions.set(t, login);
  res.json({ token: t, user });
});

// Вход по номеру: тот же логин, новая сессия
app.post("/api/login", (req, res) => {
  const login = phones.get(norm(req.body.phone));
  if (!login) return res.status(404).json({ error: "Номер не найден. Зарегистрируйся" });
  const t = token();
  sessions.set(t, login);
  res.json({ token: t, user: users.get(login) });
});

app.get("/api/me", auth, (req, res) => res.json(users.get(req.me)));

app.get("/api/search", auth, (req, res) => {
  const q = norm(req.query.q).toLowerCase().replace(/^@/, "");
  const r = [...users.values()]
    .filter(u => u.login !== req.me && u.login.includes(q))
    .slice(0, 50)
    .map(u => ({ login: u.login, pub: u.pub, c: u.c }));
  res.json(r);
});

app.get("/api/user/:login", auth, (req, res) => {
  const u = users.get(req.params.login);
  if (!u) return res.status(404).json({ error: "not found" });
  res.json({ login: u.login, pub: u.pub, c: u.c });
});

app.get("/api/chats", auth, (req, res) => {
  const out = [];
  for (const [id, msgs] of chats) {
    const [a, b] = id.split("_");
    if (a !== req.me && b !== req.me) continue;
    const peer = a === req.me ? b : a;
    const last = msgs[msgs.length - 1];
    if (last) out.push({ peer, last });
  }
  res.json(out);
});

app.get("/api/messages/:peer", auth, (req, res) => {
  res.json(chats.get(chatId(req.me, req.params.peer)) || []);
});

app.get("/api/calls", auth, (req, res) => {
  res.json(calls.filter(c => c.a === req.me || c.b === req.me).slice(-100).reverse());
});

// Вебсокет: авторизация по токену, потом сообщения и сигналы звонков
wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://x");
  const login = sessions.get(url.searchParams.get("token"));
  if (!login) return ws.close();
  online.set(login, ws);
  ws.send(JSON.stringify({ type: "hello", login }));

  ws.on("message", raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === "msg") {
      const to = norm(m.to);
      if (!users.has(to)) return;
      const msg = { f: login, ts: Date.now(), ct: String(m.ct || "").slice(0, 20000), iv: String(m.iv || ""), s: m.s ? 1 : 0 };
      const id = chatId(login, to);
      if (!chats.has(id)) chats.set(id, []);
      chats.get(id).push(msg);
      const payload = JSON.stringify({ type: "msg", peer: login, msg });
      const peerWs = online.get(to);
      if (peerWs && peerWs.readyState === 1) peerWs.send(payload);
      ws.send(JSON.stringify({ type: "msg", peer: to, msg }));
    }

    if (m.type === "call") {
      // сигналы WebRTC: offer, answer, end, decline — просто пересылаем
      const to = norm(m.to);
      const target = online.get(to);
      if (m.kind === "offer") {
        const c = { id: crypto.randomUUID(), a: login, b: to, ts: Date.now(), answered: false };
        calls.push(c);
        m.callId = c.id;
      }
      if (m.kind === "answer") {
        const c = calls.find(x => x.id === m.callId);
        if (c) c.answered = true;
      }
      if (target && target.readyState === 1) target.send(JSON.stringify({ type: "call", from: login, ...m }));
      if (m.kind === "offer") ws.send(JSON.stringify({ type: "call", from: login, ...m }));
    }
  });

  ws.on("close", () => { if (online.get(login) === ws) online.delete(login); });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log("SLIMOCnat on", PORT));
