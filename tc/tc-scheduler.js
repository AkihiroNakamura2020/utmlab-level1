// traffic-controller/tc-scheduler.js
// Traffic Controller: capacity を見ながら複数フライトをスケジューリングする簡易版

import { baseCost } from "./tc-map.js";
import { findShortestPath, printRoute } from "./tc-router.js";

// ------------------------------
// エッジごとの使用状況テーブル
//   usage[edgeId][timeSlotSec] = その秒に通過中の機体数
// ------------------------------
const edgeUsage = new Map(); // edgeId -> Map<timeSlot, count>

// env で上書き可能（dev.env: MAX_DEPART_DELAY_SEC=10）
// 大きくするほど混雑時に待機してスロットを探す時間が延びる
// 小さくするほど即拒否になり NO_SLOT_IN_WINDOW が増える
const MAX_DEPART_DELAY_SEC = Number(process.env.MAX_DEPART_DELAY_SEC ?? 10);

const ROUTE_MAP_MODE = process.env.ROUTE_MAP_MODE || "warn"; // "warn" | "reject"
const PREFER_REQUESTED_DEPART =
  (process.env.TC_PREFER_REQUESTED_DEPART || "true") === "true";

// ------------------------------
// run context (TC is time source of truth)
// ------------------------------
// 絶対時刻は TC が生成する。相対秒の基準(t0EpochMs)を TC が固定で持つ。
const TC_T0_EPOCH_MS = Number(process.env.T0_EPOCH_MS) || Date.now();
const TC_RUN_ID = process.env.RUN_ID || `run-${TC_T0_EPOCH_MS}`;

/**
 * ISO8601文字列を unix epoch(ms) に変換する。
 * パース失敗時は null を返す。
 *
 * @param {string|null|undefined} iso - ISO8601文字列
 * @returns {number|null}
 */
function parseAbsMsFromIso(iso) {
  if (!iso) return null;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

/**
 * フライトの希望出発時刻を TC の t0EpochMs 基準の相対秒へ変換する。
 *
 * 優先順：
 * 1. flight.requestedDepartAbsMs（絶対ms）
 * 2. flight.requestedDepartIso（ISO文字列 → ms変換）
 * 3. flight.depart（従来の相対秒フォールバック）
 * 4. 上記すべて無ければ 0
 *
 * 変換結果が t0 より前の場合は 0 にクランプし、警告ログを出す。
 *
 * @param {object} flight - スケジュール対象フライト
 * @param {number} t0EpochMs - TC の時間基準点(ms)
 * @returns {number} 相対秒（0以上）
 */
// TC SoT(t0EpochMs) を基準に、requestedDepartIso/AbsMs を相対秒へ
function decideDepartSec(flight, t0EpochMs) {
  if (PREFER_REQUESTED_DEPART) {
    const absMs =
      (typeof flight?.requestedDepartAbsMs === "number" &&
        Number.isFinite(flight.requestedDepartAbsMs) &&
        flight.requestedDepartAbsMs) ||
      parseAbsMsFromIso(flight?.requestedDepartIso);

    if (typeof absMs === "number" && Number.isFinite(absMs)) {
      const sec = (absMs - t0EpochMs) / 1000;
      if (sec < 0) {
        console.log("[TC][REQUESTED_DEPART_BEFORE_T0]", {
          flightId: flight?.id ?? "(no-id)",
          requestedDepartIso: flight?.requestedDepartIso ?? null,
          absMs,
          t0EpochMs,
          sec,
        });
      }
      return Math.max(0, sec);
    }
  }

  // フォールバック：従来 depart
  if (typeof flight?.depart === "number" && Number.isFinite(flight.depart)) {
    return flight.depart;
  }

  return 0;
}

/**
 * edgeUsage をリセットする。
 * 実験の再起動時や単体テスト時に使用する。
 *
 * @returns {void}
 */
export function resetUsage() {
  edgeUsage.clear();
}

/**
 * 指定エッジの usage マップを取得する。
 * 存在しない場合は空の Map を生成して登録してから返す。
 *
 * @param {string} edgeId
 * @returns {Map<number, number>} timeSlotSec -> 使用中機体数
 */
// 指定エッジの usage マップを確保
function getEdgeUsage(edgeId) {
  if (!edgeUsage.has(edgeId)) {
    edgeUsage.set(edgeId, new Map());
  }
  return edgeUsage.get(edgeId);
}

/**
 * 指定エッジを [tStart, tEnd) の時間範囲で使用しても capacity を超えないかチェックする。
 *
 * 内部では1秒刻みのスロットで評価する。
 * いずれか1秒でも capacity に達していれば false を返す。
 *
 * @param {{ id: string, capacity?: number }} edge - チェック対象エッジ
 * @param {number} tStart - 使用開始時刻（相対秒）
 * @param {number} tEnd - 使用終了時刻（相対秒、exclusive）
 * @returns {boolean} 使用可能なら true
 */
/**
 * そのエッジを [tStart, tEnd) の間使っても capacity を超えないかチェック
 * - tStart, tEnd は秒（float）想定
 * - 内部ではざっくり 1秒刻みでチェック
 */
function canUseEdge(edge, tStart, tEnd) {
  const usage = getEdgeUsage(edge.id);
  const cap = edge.capacity ?? 1;

  // [tStart, tEnd) にかかる整数秒スロットをざっくりチェック
  const from = Math.floor(tStart);
  const to = Math.ceil(tEnd); // この手前まで

  for (let t = from; t < to; t++) {
    const used = usage.get(t) ?? 0;
    if (used >= cap) {
      return false; // どこか1秒でも満杯ならNG
    }
  }
  return true;
}

/**
 * 指定エッジを [tStart, tEnd) の時間範囲で予約する（使用カウントをインクリメント）。
 * canUseEdge で確認してから呼ぶこと。
 *
 * @param {{ id: string, capacity?: number }} edge - 予約対象エッジ
 * @param {number} tStart - 予約開始時刻（相対秒）
 * @param {number} tEnd - 予約終了時刻（相対秒、exclusive）
 * @returns {void}
 */
/**
 * エッジを [tStart, tEnd) の間「予約」する
 */
function reserveEdge(edge, tStart, tEnd) {
  const usage = getEdgeUsage(edge.id);
  const from = Math.floor(tStart);
  const to = Math.ceil(tEnd);

  for (let t = from; t < to; t++) {
    const used = usage.get(t) ?? 0;
    usage.set(t, used + 1);
  }
}

/**
 * 1フライト分のスケジューリングを実行する。
 *
 * 処理の流れ：
 * 1. from/to の解決（routeHint フォールバック含む）
 * 2. decideDepartSec で希望出発時刻を相対秒へ変換
 * 3. findShortestPath でルート計算
 * 4. 各エッジを順に「capacity に空きがある最早スロット」に予約
 *    - 1秒ずつずらして空きを探す
 *    - MAX_DEPART_DELAY_SEC を超えたら NO_SLOT_IN_WINDOW で失敗
 * 5. runId / scheduleBasis / actualDepartAbsMs / arrivalAbsMs / segments を組み立てて返す
 *
 * @param {{ id?: string, from?: string, to?: string, depart?: number,
 *           requestedDepartIso?: string, requestedDepartAbsMs?: number,
 *           routeHint?: { tcFrom?: string, tcTo?: string } }} flight
 * @returns {{ ok: true, flightId: string, runId: string,
 *             scheduleBasis: { t0EpochMs: number },
 *             from: string, to: string,
 *             requestedDepart: number, actualDepart: number, arrivalTime: number,
 *             actualDepartAbsMs: number, arrivalAbsMs: number,
 *             segments: Array<{ edgeId: string, from: string, to: string,
 *                               tStart: number, tEnd: number, travelTime: number,
 *                               capacity: number,
 *                               tStartAbsMs: number, tEndAbsMs: number }>,
 *             routeNodes: string[] }
 *           | { ok: false, reason: string, flightId: string, error?: string }}
 */
/**
 * 1フライト分のスケジューリング
 * - flight: { id, from, to, depart } （depart は希望出発時刻[秒]）
 * - 返り値: スケジュール結果オブジェクト
 */
function scheduleOneFlight(flight) {
  console.log("[TC][SCHEDULE_ONE_FLIGHT] Start", {
    id: flight?.id,
    from: flight?.from,
    to: flight?.to,
  });

  const id = flight?.id ?? "(no-id)";

  // route / from / to
  const from = flight?.from || flight?.routeHint?.tcFrom || null;
  const to = flight?.to || flight?.routeHint?.tcTo || null;

  // ★ルート欠損ポリシー
  // ★ルート欠損ポリシー（warnでも続行しない）
  if (!from || !to) {
    const payload = {
      id,
      from,
      to,
      routeMapMode: ROUTE_MAP_MODE,
      hint: flight?.routeHint ?? null,
    };

    if (ROUTE_MAP_MODE === "reject") {
      throw new Error(`[TC][ROUTE_MISSING] ${JSON.stringify(payload)}`);
    } else {
      console.log("[TC][ROUTE_MISSING]", payload);
      return { ok: false, reason: "ROUTE_MISSING", flightId: id };
    }
  }

  // departSec 決定（requested→t0差分→sec を優先、なければ従来 depart）
  const depart = decideDepartSec(flight, TC_T0_EPOCH_MS);

  console.log("[TC][NEW_FLIGHT]", {
    id,
    from,
    to,
    depart,

    // ★デバッグ可視化
    requestedDepartIso: flight?.requestedDepartIso ?? null,
    requestedDepartAbsMs:
      typeof flight?.requestedDepartAbsMs === "number"
        ? flight.requestedDepartAbsMs
        : null,
    t0EpochMs: TC_T0_EPOCH_MS,
    preferRequestedDepart: PREFER_REQUESTED_DEPART,
  });

  // まずは最短時間ルートを計算
  const routeResult = findShortestPath(from, to); //totalcost,nodes,edgesを格納

  if (!routeResult.ok) {
    return {
      ok: false,
      reason: `route_unreachable: ${routeResult.reason}`,
      flightId: id,
    };
  }

  // 各エッジに対して「capacity に収まる時間」を前から順に探していく
  let currentTime = depart;
  const segments = [];

  for (const edge of routeResult.pathEdges) {
    let travelTime;
    try {
      travelTime = baseCost(edge);
    } catch (e) {
      console.error("[TC][EDGE_COST_THROWN]", {
        flightId: id,
        edgeId: edge?.id ?? null,
        error: String(e?.message || e),
        stack: e?.stack ?? null,
      });
      return {
        ok: false,
        reason: "EDGE_COST_THROWN",
        flightId: id,
        error: String(e?.message || e),
      };
    }

    if (!Number.isFinite(travelTime) || travelTime <= 0) {
      console.error("[TC][EDGE_COST_INVALID]", {
        flightId: id,
        edgeId: edge.id,
        travelTime,
      });
      return {
        ok: false,
        reason: "EDGE_COST_INVALID",
        flightId: id,
      };
    }

    let tStart = currentTime;

    // capacity が空いている時間を探す（1秒ずつ後ろにずらすシンプル方式）
    while (!canUseEdge(edge, tStart, tStart + travelTime)) {
      // ★追加：待ちすぎたらこのフライトは諦める
      if (tStart - depart > MAX_DEPART_DELAY_SEC) {
        console.log(
          `[TC][REJECT_WINDOW] flight=${id} edge=${edge.id}` +
            ` depart=${depart} tStart=${tStart} delay=${tStart - depart}` +
            ` maxDelay=${MAX_DEPART_DELAY_SEC}`
        );

        return {
          ok: false,
          reason: "NO_SLOT_IN_WINDOW",
          flightId: id,
        };
      }

      tStart += 1; // 1秒待機
    }

    const tEnd = tStart + travelTime;
    reserveEdge(edge, tStart, tEnd);

    segments.push({
      edgeId: edge.id,
      from: edge.from,
      to: edge.to,
      tStart,
      tEnd,
      travelTime,
      capacity: edge.capacity,
    });

    currentTime = tEnd; // 次のエッジはこの時刻からスタート
  }

  const actualDepart = segments[0]?.tStart ?? depart;
  const arrivalTime = segments[segments.length - 1]?.tEnd ?? depart;

  // ★ returnする結果を先に組み立てる（NaNチェックで参照するため）
  const result = {
    ok: true,
    flightId: id,

    // ★監査/トレーサビリティ（TCが決める）
    runId: TC_RUN_ID,
    scheduleBasis: { t0EpochMs: TC_T0_EPOCH_MS },

    from,
    to,
    requestedDepart: depart,
    actualDepart,
    arrivalTime,

    actualDepartAbsMs: TC_T0_EPOCH_MS + Math.round(actualDepart * 1000),
    arrivalAbsMs: TC_T0_EPOCH_MS + Math.round(arrivalTime * 1000),

    segments: segments.map((s) => ({
      ...s,
      tStartAbsMs: TC_T0_EPOCH_MS + Math.round(s.tStart * 1000),
      tEndAbsMs: TC_T0_EPOCH_MS + Math.round(s.tEnd * 1000),
    })),

    routeNodes: routeResult.pathNodes,
  };

  // ★デバッグ用 NaN 検出（env ガード）
  if (process.env.TC_DEBUG_NAN === "true") {
    if (
      !Number.isFinite(result.actualDepartAbsMs) ||
      !Number.isFinite(result.arrivalAbsMs)
    ) {
      console.error("[TC][NaN_DETECTED_IN_SCHEDULER]", {
        flightId: id,
        t0EpochMs: TC_T0_EPOCH_MS,
        actualDepart,
        arrivalTime,
        actualDepartAbsMs: result.actualDepartAbsMs,
        arrivalAbsMs: result.arrivalAbsMs,
        routeNodes: routeResult?.pathNodes ?? null,
        segments: result.segments.length,
      });
    }
  }

  return result;
}

/**
 * 複数フライトを順番にスケジューリングする。
 *
 * 各フライトは scheduleOneFlight に委譲し、例外は catch して
 * { ok: false, reason: "SCHEDULE_FAILED" } として積む。
 * 1件の失敗が他フライトのスケジューリングを止めない設計。
 *
 * 戻り値の runId / scheduleBasis は TC の SoT（時間の正）として
 * FIMS / UASSP が検証に使う。
 *
 * @param {Array<{ id?: string, from?: string, to?: string, depart?: number,
 *                 requestedDepartIso?: string, requestedDepartAbsMs?: number,
 *                 routeHint?: { tcFrom?: string, tcTo?: string } }>} flights
 * @returns {{ ok: true, runId: string,
 *             scheduleBasis: { t0EpochMs: number },
 *             results: Array<ReturnType<typeof scheduleOneFlight>> }}
 */
/**
 * 複数フライトを順番にスケジューリング
 * - flights は、スケジュールしたい順番に並んでいるとする
 */
export function scheduleFlights(flights) {
  const results = [];

  for (const f of Array.isArray(flights) ? flights : []) {
    try {
      const r = scheduleOneFlight(f);
      // scheduleOneFlight は ok:true/false を返すので、そのまま積む
      results.push(r);
    } catch (e) {
      console.error("[TC][SCHEDULE_EXCEPTION]", {
        flightId: f?.id ?? "(no-id)",
        error: String(e?.message || e),
        stack: e?.stack ?? null,
      });

      results.push({
        ok: false,
        flightId: f?.id ?? "(no-id)",
        reason: "SCHEDULE_FAILED",
        error: String(e?.message || e),
      });
    }
  }

  return {
    ok: true,
    runId: TC_RUN_ID,
    scheduleBasis: { t0EpochMs: TC_T0_EPOCH_MS },
    results,
  };
}

// ------------------------------
// 結果表示ヘルパ
// ------------------------------
/**
 * scheduleFlights の戻り値を人間が読みやすい形でコンソールに出力する。
 * デバッグ・CLI 実行時に使用する。実装ロジックには影響しない。
 *
 * @param {{ ok: boolean, runId?: string,
 *            scheduleBasis?: { t0EpochMs: number },
 *            results?: Array<ReturnType<typeof scheduleOneFlight>> }} resp
 * @returns {void}
 */
export function printSchedule(resp) {
  console.log("=== Traffic Schedule Result ===");
  if (!resp || !Array.isArray(resp.results)) {
    console.log("[TC] invalid schedule response");
    return;
  }

  console.log(
    "[TC] runId:",
    resp.runId,
    "t0EpochMs:",
    resp.scheduleBasis?.t0EpochMs
  );

  for (const r of resp.results) {
    if (!r.ok) {
      console.log(
        `Flight ${r.flightId || r.flightId || "(no-id)"}: FAILED (${r.reason})`
      );
      continue;
    }

    const delay = r.actualDepart - r.requestedDepart;
    console.log(`Flight ${r.flightId}: ${r.from} -> ${r.to}`);
    console.log(
      `  requestedDepart=${r.requestedDepart.toFixed(1)}s` +
        ` actualDepart=${r.actualDepart.toFixed(1)}s` +
        ` arrival=${r.arrivalTime.toFixed(1)}s` +
        ` delay=${delay.toFixed(1)}s`
    );
    console.log(`  route: ${r.routeNodes.join(" -> ")}`);
    console.log("  segments:");
    for (const seg of r.segments) {
      console.log(
        `    ${seg.edgeId}: ${seg.from} -> ${seg.to}` +
          ` [${seg.tStart.toFixed(1)}s ~ ${seg.tEnd.toFixed(1)}s]` +
          ` (travel=${seg.travelTime.toFixed(1)}s, cap=${seg.capacity})`
      );
    }
  }
}

// ------------------------------
// CLI: node traffic-controller/tc-scheduler.js
// ------------------------------
if (import.meta.url === `file://${process.argv[1]}`) {
  // 簡単なテストシナリオ：
  const flights = [
    { id: "F1", from: "A", to: "D", depart: 0 },
    { id: "F2", from: "B", to: "E", depart: 0 },
    { id: "F3", from: "C", to: "D", depart: 2 },
    { id: "F4", from: "E", to: "B", depart: 4 },
  ];

  const results = scheduleFlights(flights);
  printSchedule(results);
}
