// traffic-controller/tc-api.js
// Traffic Controller を HTTP API として公開する
// POST /schedule で flights を受け取り、tc-scheduler.js でスケジュールして返す

import express from "express";
import { scheduleFlights, resetUsage } from "./tc-scheduler.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.TC_PORT || 4100;

// ============================================================
// NDJSON ログ関数
// ============================================================
const LOGDIR = path.join(__dirname, "../logs");
fs.mkdirSync(LOGDIR, { recursive: true });
const LOGFILE = path.join(LOGDIR, "tc.ndjson");

function nowISO() {
  return new Date().toISOString();
}

function logTC(msg, extra = {}) {
  const entry = { ts: nowISO(), source: "TC", msg, ...extra };
  try {
    fs.appendFileSync(LOGFILE, JSON.stringify(entry) + "\n");
    console.log(`[TC LOG] ${msg}`, extra);
  } catch (e) {
    console.error("[TC LOG WRITE ERROR]", e.message);
  }
}

/**
 * POST /reset
 * edgeUsage を全クリアして次の実験に備える。
 * 実験開始前（sim_multi.py の notify_ui_experiment_start から）呼ぶこと。
 * 呼ばないと前実験のスロット予約が残り NO_SLOT_IN_WINDOW が増加する。
 */
app.post("/reset", (req, res) => {
  resetUsage();
  logTC("EDGE_USAGE_RESET", { reason: req.body?.reason || "experiment_start" });
  res.json({ ok: true });
});

app.post("/schedule", (req, res) => {
  try {
    const { flights } = req.body || {};

    logTC("SCHEDULE_IN", {
      runId: process.env.RUN_ID || null,
      t0EpochMs: Number(process.env.T0_EPOCH_MS) || null,
      flightsCount: Array.isArray(flights) ? flights.length : null,
      flightIds: Array.isArray(flights)
        ? flights.map((f) => f?.id ?? "(no-id)")
        : null,
      routeMapMode: process.env.ROUTE_MAP_MODE || "warn",
      preferRequestedDepart:
        (process.env.TC_PREFER_REQUESTED_DEPART || "true") === "true",
    });

    if (!Array.isArray(flights)) {
      return res.status(400).json({
        ok: false,
        error: "flights must be an array",
      });
    }

    // 今は 1 リクエスト = 1 セッション として、その場限りでスケジュール
    const scheduleResult = scheduleFlights(flights);
    const results = scheduleResult.results || [];

    const okCount = results.filter((r) => r?.ok).length;
    const failCount = results.length - okCount;

    logTC("SCHEDULE_OUT", {
      resultsCount: results.length,
      okCount,
      failCount,
      runId: scheduleResult.runId,
      scheduleBasis: scheduleResult.scheduleBasis,
      absSummary: results
        .filter((r) => r?.ok)
        .map((r) => ({
          flightId: r.flightId,
          runId: r.runId || null,
          t0EpochMs: r.scheduleBasis?.t0EpochMs ?? null,
          actualDepartAbsMs: r.actualDepartAbsMs ?? null,
          arrivalAbsMs: r.arrivalAbsMs ?? null,
        })),
    });

    // ------------------------------
    // ★互換レスポンス（FIMSが単体直参照できる形）
    // - flights が 1 件のときだけ、results[0] をトップへ展開する
    // - 複数便は従来どおり results 配列
    // ------------------------------
    if (flights.length === 1) {
      const r0 = results[0] || null;

      // r0 が無い＝異常（防御）
      if (!r0) {
        logTC("SCHEDULE_ERROR", {
          error: "missing schedule result",
          flightsCount: flights.length,
        });
        return res.status(500).json({
          ok: false,
          error: "missing schedule result",
        });
      }

      // FIMS互換：ok=false のときも reason を返す
      if (!r0.ok) {
        logTC("SCHEDULE_FAILED", {
          flightId: r0.flightId || flights[0]?.id || "(no-id)",
          reason: r0.reason || "SCHEDULE_FAILED",
        });
        return res.json({
          ok: false,
          reason: r0.reason || "SCHEDULE_FAILED",
          flightId: r0.flightId || flights[0]?.id || "(no-id)",
          runId: scheduleResult.runId,
          scheduleBasis: scheduleResult.scheduleBasis,
        });
      }

      const isFiniteNum = (v) => typeof v === "number" && Number.isFinite(v);

      if (!isFiniteNum(r0.actualDepartAbsMs) || !isFiniteNum(r0.arrivalAbsMs)) {
        logTC("ABS_MISSING_OR_INVALID", {
          flightId: r0.flightId,
          actualDepartAbsMs: r0.actualDepartAbsMs ?? null,
          arrivalAbsMs: r0.arrivalAbsMs ?? null,
          runId: r0.runId || scheduleResult.runId,
          t0EpochMs:
            r0.scheduleBasis?.t0EpochMs ??
            scheduleResult.scheduleBasis?.t0EpochMs ??
            null,
        });

        return res.json({
          ok: false,
          reason: "ABS_MISSING_OR_INVALID",
          flightId: r0.flightId || flights[0]?.id || "(no-id)",
          runId: scheduleResult.runId,
          scheduleBasis: scheduleResult.scheduleBasis,
        });
      }

      // ok=true：単体直返し（FIMSが tc.from 等を直接参照できる）
      return res.json({
        ok: true,
        runId: r0.runId || scheduleResult.runId,
        scheduleBasis: r0.scheduleBasis || scheduleResult.scheduleBasis,
        ...r0,
      });
    }

    // 複数便：results配列レスポンス（既存）
    res.json({
      ok: true,
      runId: scheduleResult.runId,
      scheduleBasis: scheduleResult.scheduleBasis,
      results,
    });
  } catch (e) {
    logTC("SCHEDULE_EXCEPTION", {
      error: e.message || String(e),
      stack: e.stack,
    });
    console.error("[TC] /schedule error:", e);
    res.status(500).json({
      ok: false,
      error: e.message || String(e),
    });
  }
});

app.listen(PORT, () => {
  console.log(`[TC] listening on http://localhost:${PORT}`);
  logTC("SERVER_STARTED", {
    port: PORT,
    runId: process.env.RUN_ID || null,
    t0EpochMs: Number(process.env.T0_EPOCH_MS) || null,
  });
});
