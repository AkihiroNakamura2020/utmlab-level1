// fims/fims.js
import express from "express";
import http from "http";
import axios from "axios";

const app = express();
app.use(express.json());

// name -> webhook の登録表（メモリ）
const peers = new Map();

// UASSP登録
app.post("/register", (req, res) => {
  const { name, webhook } = req.body || {};
  console.log("[FIMS] /register:", req.body);
  if (!name || !webhook) return res.status(400).json({ error: "name and webhook required" });
  peers.set(name, webhook);             // 例: UASSP_A -> http://localhost:4001/notify
  return res.json({ ok: true, peers: [...peers.keys()] });
});

// イベント受信 → 全登録先へ転送
app.post("/events", async (req, res) => {
  const evt = req.body; // { from, type, payload, ts }
  const { from, type, ts } = evt || {};
  if (!from || !type || ts == null) return res.status(400).json({ error: "missing fields: from/type/ts" });

  console.log("[FIMS] /events body:", evt);

  // 転送（自分自身含む/除外は必要に応じて）
  const deliveries = [];
  for (const [name, url] of peers.entries()) {
    try {
      // UASSP側は /notify を待ち受け
      await axios.post(url, evt, { headers: { "Content-Type": "application/json" } });
      deliveries.push({ name, ok: true });
    } catch (e) {
      deliveries.push({ name, ok: false, err: e.message });
    }
  }
  return res.json({ ok: true, delivered: deliveries });
});

http.createServer(app).listen(3000, () => {
  console.log("[FIMS] listening on :3000");
});
