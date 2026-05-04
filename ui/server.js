/**
 * ui/server.js
 * MQTT → WebSocket ブリッジ + UTM ダッシュボード配信
 *
 * - utm/events を購読してブラウザへ転送
 * - utm/notify/# を購読してブラウザへ転送
 * - GET /api/status で activePlans / peers の現在状態を返す
 * - 静的ファイル（ui/public/）を配信
 */

import express from "express";
import { createServer } from "http";
import { WebSocketServer } from "ws";
import mqtt from "mqtt";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.UI_PORT || 8080;
const BROKER_URL = process.env.BROKER_URL || "mqtt://localhost:1883";

// ブラウザへ転送するMQTTトピック
const WATCH_TOPICS = ["utm/events", "utm/notify/#"];

// SLA 計測用（最新実験分のみ保持）
const slaBuffer = []; // { flightPlanId, filedMs, notifyMs, delayMs }
const MAX_SLA = 200;

// タイムライン（最新50件）
const timeline = [];
const MAX_TIMELINE = 50;

// --- Express + HTTP + WebSocket セットアップ ---
const app = express();
app.use(express.static(path.join(__dirname, "public")));

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer });

/**
 * 接続中のブラウザ全員へメッセージを送る。
 *
 * @param {object} msg - JSON シリアライズ可能なオブジェクト
 */
function broadcast(msg) {
  const payload = JSON.stringify(msg);
  for (const client of wss.clients) {
    if (client.readyState === 1 /* OPEN */) {
      client.send(payload);
    }
  }
}

// --- GET /api/status ---
app.get("/api/status", (_req, res) => {
  // SLA P50/P95
  const delays = slaBuffer.map((s) => s.delayMs).sort((a, b) => a - b);
  const n = delays.length;
  const p50 = n > 0 ? delays[Math.floor(n * 0.5)] : null;
  const p95 = n > 0 ? delays[Math.floor(n * 0.95)] : null;

  res.json({
    sla: { count: n, p50, p95 },
    timeline: timeline.slice(-50),
  });
});

// --- MQTT 接続 ---
const mqttClient = mqtt.connect(BROKER_URL, {
  clientId: "UTM_UI_BRIDGE",
  reconnectPeriod: 2000,
});

mqttClient.on("connect", () => {
  console.log(`[UI] MQTT connected: ${BROKER_URL}`);
  for (const topic of WATCH_TOPICS) {
    mqttClient.subscribe(topic);
    console.log(`[UI] subscribed: ${topic}`);
  }
});

mqttClient.on("error", (e) => console.error("[UI MQTT ERROR]", e.message));

mqttClient.on("message", (topic, msgBuf) => {
  let envelope;
  try {
    envelope = JSON.parse(msgBuf.toString("utf8"));
  } catch {
    return;
  }

  const evtType = envelope?.type || "(unknown)";
  const traceId = envelope?.traceId || null;
  const fpId =
    envelope?.payload?.flightPlanId ||
    envelope?.payload?.subject?.flightPlanId ||
    envelope?.flightPlanId ||
    null;

  // タイムライン更新
  const entry = {
    ts: Date.now(),
    topic,
    type: evtType,
    source: envelope?.source || null,
    traceId,
    flightPlanId: fpId,
  };
  timeline.push(entry);
  if (timeline.length > MAX_TIMELINE) timeline.shift();

  // SLA: FLIGHT_PLAN_FILED の受信時刻を記録
  if (evtType === "FLIGHT_PLAN_FILED" && fpId) {
    const existing = slaBuffer.find((s) => s.flightPlanId === fpId);
    if (!existing) {
      slaBuffer.push({ flightPlanId: fpId, filedMs: Date.now(), notifyMs: null, delayMs: null });
      if (slaBuffer.length > MAX_SLA) slaBuffer.shift();
    }
  }

  // SLA: utm/notify/# に FLIGHT_PLAN_FILED が来たら到着時刻を記録
  if (topic.startsWith("utm/notify/") && evtType === "FLIGHT_PLAN_FILED" && fpId) {
    const rec = slaBuffer.find((s) => s.flightPlanId === fpId && s.notifyMs === null);
    if (rec) {
      rec.notifyMs = Date.now();
      rec.delayMs = rec.notifyMs - rec.filedMs;
    }
  }

  // ブラウザへブロードキャスト
  broadcast({ type: "mqtt", topic, envelope });
});

// --- WebSocket 接続ハンドラ ---
wss.on("connection", (ws) => {
  console.log("[UI] browser connected");

  // 接続直後に現在のタイムラインを送る
  const delays = slaBuffer.map((s) => s.delayMs).filter((d) => d != null).sort((a, b) => a - b);
  const n = delays.length;
  ws.send(JSON.stringify({
    type: "init",
    timeline: timeline.slice(-50),
    sla: {
      count: n,
      p50: n > 0 ? delays[Math.floor(n * 0.5)] : null,
      p95: n > 0 ? delays[Math.floor(n * 0.95)] : null,
    },
  }));

  ws.on("error", (e) => console.error("[UI WS ERROR]", e.message));
});

// --- 起動 ---
httpServer.listen(PORT, () => {
  console.log(`[UI] Dashboard: http://localhost:${PORT}`);
});
