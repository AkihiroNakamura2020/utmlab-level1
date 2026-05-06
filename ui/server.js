/**
 * ui/server.js - UTM Dashboard: MQTT→WebSocket bridge + state tracking
 *
 * 4フェーズ SLA 計測:
 *   REQUEST → ASSIGNED (TC+SDSP処理)
 *   ASSIGNED → FILED   (UASSP往復)
 *   FILED → NOTIFY     (FIMS衝突判定+署名+通知)
 *   REQUEST → DONE     (フライト総時間)
 */

import express from "express";
import { createServer } from "http";
import { WebSocketServer } from "ws";
import mqtt from "mqtt";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT       = process.env.UI_PORT    || 8080;
const BROKER_URL = process.env.BROKER_URL || "mqtt://localhost:1883";

// --- State ---
const traces      = new Map(); // traceId → TraceRec
const activePlans = new Map(); // fpId    → PlanRec
const rejectReasons = {};      // reason  → count
const conflictLog   = [];      // 最新 MAX_CONFLICTS 件
const timeline      = [];      // 最新 MAX_TIMELINE 件

const MAX_TIMELINE  = 100;
const MAX_TRACES    = 500;
const MAX_CONFLICTS = 50;

/**
 * 実験メタデータ。
 * sim_multi.py が POST /api/experiment/start で通知する。
 * 環境変数 EXPECTED_FLIGHTS をフォールバックとして使う。
 */
let experiment = {
  expectedTotal:    Number(process.env.EXPECTED_FLIGHTS || 0),
  flightsPerUassp:  0,
  uasspCount:       2,
  startedAt:        null,
  runId:            null,
};

/**
 * traceId に対応する TraceRec を取得または新規作成する。
 * @param {string|null} traceId
 * @param {string|null} fpId
 * @param {string|null} source
 * @returns {object|null}
 */
function ensureTrace(traceId, fpId, source) {
  if (!traceId) return null;
  if (!traces.has(traceId)) {
    if (traces.size >= MAX_TRACES) traces.delete(traces.keys().next().value);
    traces.set(traceId, {
      traceId,
      fpId:        null,
      source:      null,
      requestMs:   null,
      assignedMs:  null,
      filedMs:     null,
      notifyMs:    null,
      completedMs: null,
      rejectedMs:  null,
      reason:      null,
      done:        false,
    });
  }
  const tr = traces.get(traceId);
  if (fpId   && !tr.fpId)   tr.fpId   = fpId;
  if (source && !tr.source) tr.source = source;
  return tr;
}

/**
 * envelope から flightPlanId を抽出する。
 * @param {object} envelope
 * @returns {string|null}
 */
function extractFpId(envelope) {
  const p = envelope?.payload || {};
  return p.flightPlanId
      || p.subject?.flightPlanId
      || envelope?.flightPlanId
      || null;
}

/**
 * 配列の統計値（count / min / avg / p50 / p95 / max）を計算する。
 * @param {number[]} arr
 * @returns {object|null}
 */
function statsOf(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const n = s.length;
  return {
    count: n,
    min:   s[0],
    avg:   Math.round(s.reduce((a, b) => a + b, 0) / n),
    p50:   s[Math.floor(n * 0.5)],
    p95:   s[Math.floor(n * 0.95)],
    max:   s[n - 1],
  };
}

/**
 * traces から全4フェーズの SLA を計算して返す。
 * @returns {object}
 */
function computeSla() {
  const reqToAsgn = [], asgnToFiled = [], filedToNotify = [], reqToDone = [], reqToReject = [];
  for (const tr of traces.values()) {
    if (tr.requestMs  && tr.assignedMs)  reqToAsgn.push(tr.assignedMs  - tr.requestMs);
    if (tr.assignedMs && tr.filedMs)     asgnToFiled.push(tr.filedMs   - tr.assignedMs);
    if (tr.filedMs    && tr.notifyMs)    filedToNotify.push(tr.notifyMs - tr.filedMs);
    if (tr.requestMs  && tr.completedMs) reqToDone.push(tr.completedMs - tr.requestMs);
    if (tr.requestMs  && tr.rejectedMs)  reqToReject.push(tr.rejectedMs - tr.requestMs);
  }
  return {
    reqToAsgn:     statsOf(reqToAsgn),
    asgnToFiled:   statsOf(asgnToFiled),
    filedToNotify: statsOf(filedToNotify),
    reqToDone:     statsOf(reqToDone),
    reqToReject:   statsOf(reqToReject),
  };
}

/**
 * 現在の全状態スナップショットを返す（/api/status + WS init で使用）。
 * @returns {object}
 */
function currentState() {
  const allTraces = [...traces.values()];
  const observed  = traces.size;
  const expected  = experiment.expectedTotal;
  return {
    activePlans:   [...activePlans.values()],
    traces:        allTraces.slice(-100),
    rejectedTraces: allTraces.filter(t => t.rejectedMs)
                             .sort((a, b) => b.rejectedMs - a.rejectedMs)
                             .slice(0, 30),
    sla:           computeSla(),
    rejectReasons: { ...rejectReasons },
    conflicts:     conflictLog.slice(-20),
    timeline:      timeline.slice(-50),
    experiment: {
      ...experiment,
      observedTotal: observed,
      missingTotal:  expected ? Math.max(0, expected - observed) : null,
      captureRate:   expected ? Math.round(observed / expected * 100) : null,
    },
    stats: {
      observed,
      expected,
      active:    activePlans.size,
      completed: allTraces.filter(t => t.completedMs).length,
      rejected:  allTraces.filter(t => t.rejectedMs).length,
    },
  };
}

// --- Express + HTTP + WebSocket ---
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));
app.get("/api/status", (_req, res) => res.json(currentState()));

/**
 * 実験開始通知エンドポイント。
 * sim_multi.py が実験開始前に POST して expectedTotal を UIサーバーに伝える。
 * 受信時に前実験の traces / activePlans / conflicts / timeline をリセットする。
 */
app.post("/api/experiment/start", (req, res) => {
  const { expectedTotal, flightsPerUassp, uasspCount, runId } = req.body || {};
  experiment = {
    expectedTotal:   Number(expectedTotal || 0),
    flightsPerUassp: Number(flightsPerUassp || 0),
    uasspCount:      Number(uasspCount || 2),
    startedAt:       Date.now(),
    runId:           runId || null,
  };
  // 前実験の状態をリセット
  traces.clear();
  activePlans.clear();
  conflictLog.length = 0;
  timeline.length = 0;
  Object.keys(rejectReasons).forEach(k => delete rejectReasons[k]);

  console.log(`[UI] experiment/start: expected=${experiment.expectedTotal} flights`);
  broadcast({ type: "experiment_started", experiment: currentState().experiment });
  res.json({ ok: true, experiment });
});

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer });

function broadcast(msg) {
  const payload = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(payload);
}

wss.on("connection", (ws) => {
  console.log("[UI] browser connected");
  ws.send(JSON.stringify({ type: "init", ...currentState() }));
  ws.on("error", (e) => console.error("[UI WS ERROR]", e.message));
});

// --- MQTT ---
const mqttClient = mqtt.connect(BROKER_URL, {
  clientId: "UTM_UI_BRIDGE",
  reconnectPeriod: 2000,
});

mqttClient.on("connect", () => {
  console.log(`[UI] MQTT connected: ${BROKER_URL}`);
  for (const t of ["utm/events", "utm/notify/#"]) {
    mqttClient.subscribe(t);
    console.log(`[UI] subscribed: ${t}`);
  }
});
mqttClient.on("error", (e) => console.error("[UI MQTT ERROR]", e.message));

mqttClient.on("message", (topic, msgBuf) => {
  let envelope;
  try { envelope = JSON.parse(msgBuf.toString("utf8")); } catch { return; }

  const now     = Date.now();
  const evtType = envelope?.type  || "(unknown)";
  const traceId = envelope?.traceId || null;
  const src     = envelope?.source  || null;
  const fpId    = extractFpId(envelope);
  const payload = envelope?.payload || {};

  // タイムライン更新（EVENT は subtype を付加して不明瞭さを解消）
  const eventSubtype = (evtType === "EVENT" && payload?.type) ? payload.type : null;
  const entry = { ts: now, topic, type: evtType, eventSubtype, source: src, traceId, flightPlanId: fpId };
  timeline.push(entry);
  if (timeline.length > MAX_TIMELINE) timeline.shift();

  const tr = ensureTrace(traceId, fpId, src);

  // インクリメンタル更新用
  let traceUpdate = null;
  let planAdd     = null;
  let planRemove  = null;
  let conflictAdd = null;

  // --- utm/events (UASSP→FIMS) ---
  if (topic === "utm/events") {
    if (evtType === "FLIGHT_PLAN_REQUEST" && tr && !tr.requestMs) {
      tr.requestMs = now;
      traceUpdate = { traceId, phase: "request", requestMs: now };
    }

    if (evtType === "FLIGHT_PLAN_FILED") {
      if (tr && !tr.filedMs) {
        tr.filedMs = now;
        traceUpdate = { traceId, phase: "filed", filedMs: now };
      }
      if (fpId) {
        const tc    = payload.tcSchedule || null;
        const route = payload.route      || {};
        const dur   = tc?.arrivalAbsMs && tc?.actualDepartAbsMs
          ? Math.round((tc.arrivalAbsMs - tc.actualDepartAbsMs) / 1000)
          : null;
        const plan = {
          fpId,
          who:       src || "?",
          from:      tc?.from || (route.start ? route.start.join(",") : "?"),
          to:        tc?.to   || (route.end   ? route.end.join(",")   : "?"),
          durationSec: dur,
          traceId,
          filedMs:   now,
        };
        activePlans.set(fpId, plan);
        planAdd = plan;
      }
    }

    if (evtType === "FLIGHT_PLAN_COMPLETED") {
      if (tr && !tr.completedMs) {
        tr.completedMs = now;
        tr.done = true;
        traceUpdate = { traceId, phase: "completed", completedMs: now };
      }
      if (fpId) { activePlans.delete(fpId); planRemove = fpId; }
    }
  }

  // --- utm/notify/# (FIMS→UASSP) ---
  if (topic.startsWith("utm/notify/")) {
    if (evtType === "FLIGHT_PLAN_ASSIGNED" && tr && !tr.assignedMs) {
      tr.assignedMs = now;
      traceUpdate = { traceId, phase: "assigned", assignedMs: now };
    }

    if (evtType === "FLIGHT_PLAN_FILED" && tr && !tr.notifyMs) {
      tr.notifyMs = now;
      traceUpdate = { traceId, phase: "notify", notifyMs: now };
    }

    if (evtType === "PLAN_REJECTED") {
      const reason = payload.reason || "(unknown)";
      rejectReasons[reason] = (rejectReasons[reason] || 0) + 1;
      if (tr && !tr.rejectedMs) {
        tr.rejectedMs = now;
        tr.reason     = reason;
        tr.done       = true;
        traceUpdate   = { traceId, phase: "rejected", rejectedMs: now, reason };
      }
      if (fpId) { activePlans.delete(fpId); planRemove = fpId; }
    }

    // SEGMENT_CONFLICT_GUARD イベント
    if (evtType === "EVENT" && payload?.type === "SEGMENT_CONFLICT_GUARD") {
      const subj = payload.subject || {};
      const det  = payload.details || {};
      const conf = {
        ts:  now,
        fpId: subj.flightPlanId || "(unknown)",
        who:  subj.who || "?",
        traceId,
        conflictsWith: (det.conflictsWith || []).map(c => ({
          fpId: c.fpId,
          who:  c.who,
          edges: (c.details || []).map(d => ({
            edgeId:    d.edgeId,
            overlapMs: (d.a && d.b)
              ? Math.max(0, Math.min(d.a.tEndAbsMs, d.b.tEndAbsMs)
                          - Math.max(d.a.tStartAbsMs, d.b.tStartAbsMs))
              : null,
          })),
        })),
      };
      // 同一 fpId の conflict を短時間に重複 push しない（notifyPeers が全peer分送るため）
      const recent = conflictLog.find(c => c.fpId === conf.fpId && now - c.ts < 2000);
      if (!recent) {
        conflictLog.push(conf);
        if (conflictLog.length > MAX_CONFLICTS) conflictLog.shift();
        conflictAdd = conf;
      }
    }
  }

  // ブラウザへブロードキャスト
  const snap = currentState();
  broadcast({
    type: "mqtt",
    entry,
    traceUpdate,
    planAdd,
    planRemove,
    conflictAdd,
    sla:            snap.sla,
    rejectReasons:  snap.rejectReasons,
    rejectedTraces: snap.rejectedTraces,
    experiment:     snap.experiment,
    stats:          snap.stats,
  });
});

httpServer.listen(PORT, () => {
  console.log(`[UI] Dashboard: http://localhost:${PORT}`);
});
