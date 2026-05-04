import mqtt from "mqtt";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import readline from "readline";
import yargs from "yargs";
import jwt from "jsonwebtoken";
import axios from "axios"; // すでにあればOK
import { nearestNode } from "../tc/tc-map.js";

import {
  semanticValidateEnvelope,
  validateConflicts,
} from "./semantic-validate.js";

/**
 * MQTTでやり取りする共通envelope。
 * FIMS / UASSP / 他peer 間で流れる外側の箱。
 *
 * @typedef {object} Envelope
 * @property {string} source - 送信元サービス名。例: "FIMS", "UASSP_A"
 * @property {string} type - メッセージ種別。例: "FLIGHT_PLAN_REQUEST", "EVENT"
 * @property {string|null} [traceId] - 追跡ID。要求から完了までを束ねるID
 * @property {object} [payload] - メッセージ本体
 * @property {number} [ts] - 送信時刻(ms epoch)
 * @property {string} [jwt] - JWT署名トークン
 * @property {string} [sig] - HMAC署名
 * @property {object} [_user] - JWT verify後にFIMSが付与するデコード結果
 * @property {object} [subject] - EVENT系で使う主語情報
 */

/**
 * FLIGHT_PLAN_REQUEST / FLIGHT_PLAN_FILED などで使う飛行計画本体。
 *
 * @typedef {object} FlightPlan
 * @property {string} [flightPlanId] - フライト計画ID
 * @property {FlightRoute} [route] - ルート情報
 * @property {TcSchedule} [tcSchedule] - TCが計算したスケジュール
 * @property {string|null} [runId] - 実験/実行ID
 * @property {ScheduleBasis|null} [scheduleBasis] - 時間基準情報
 */

/**
 * 飛行ルート情報。
 * UASSPからの入力や、FIMS内部での参照に使う。
 *
 * @typedef {object} FlightRoute
 * @property {[number, number]} [start] - 出発地点 [lon, lat]
 * @property {[number, number]} [end] - 到着地点 [lon, lat]
 * @property {string} [tStart] - 希望出発時刻(ISO8601)
 * @property {number} [altMin] - 最低高度
 * @property {number} [altMax] - 最高高度
 * @property {string} [tcFrom] - TC用の開始ノード（入力で持つ場合）
 * @property {string} [tcTo] - TC用の到着ノード（入力で持つ場合）
 */

/**
 * TCの時間基準情報。
 * 相対秒を絶対時刻に変換するための基準。
 *
 * @typedef {object} ScheduleBasis
 * @property {number|null} t0EpochMs - 相対時間の基準となるepoch(ms)
 */

/**
 * TCが返すスケジュール本体。
 * FIMSではこれを SoT（時間の正）として扱う。
 *
 * @typedef {object} TcSchedule
 * @property {boolean} [ok] - TC計算成功可否
 * @property {string|null} [reason] - 失敗理由
 * @property {string|null} [flightId] - TC側のflight ID
 * @property {string|null} [runId] - 実験/実行ID
 * @property {ScheduleBasis|null} [scheduleBasis] - 絶対時間基準
 * @property {string|null} [from] - 開始ノード
 * @property {string|null} [to] - 終了ノード
 * @property {number|null} [requestedDepart] - 希望出発相対秒
 * @property {number|null} [actualDepart] - 実出発相対秒
 * @property {number|null} [arrivalTime] - 到着相対秒
 * @property {number|null} [actualDepartAbsMs] - 実出発絶対時刻(ms)
 * @property {number|null} [arrivalAbsMs] - 到着絶対時刻(ms)
 * @property {string[]} [routeNodes] - 通過ノード列
 * @property {TcSegment[]} [segments] - 区間単位スケジュール
 */

/**
 * TCが返す1区間のスケジュール。
 * 4-C判定や segment guard で使う。
 *
 * @typedef {object} TcSegment
 * @property {string|null} [edgeId] - 区間ID
 * @property {string|null} [from] - 区間開始ノード
 * @property {string|null} [to] - 区間終了ノード
 * @property {number|null} [tStart] - 区間開始相対秒
 * @property {number|null} [tEnd] - 区間終了相対秒
 * @property {number|null} [tStartAbsMs] - 区間開始絶対時刻(ms)
 * @property {number|null} [tEndAbsMs] - 区間終了絶対時刻(ms)
 */

/**
 * FLIGHT_PLAN_REQUEST を受けて FIMS が一時保持する pending 情報。
 * ASSIGNED から FILED までの橋渡しに使う。
 *
 * @typedef {object} PendingAssignment
 * @property {string|null} traceId - 要求と紐づく追跡ID
 * @property {string|null} requester - 申請元UASSP名
 * @property {number} assignedAtMs - ASSIGNEDを出した時刻(ms)
 * @property {number} expireAtMs - pending期限(ms)
 * @property {string|null} runId - 実験/実行ID
 * @property {ScheduleBasis|null} scheduleBasis - 時間基準
 * @property {TcSchedule|null} tcSchedule - ASSIGNED時点のTCスケジュール
 */

/**
 * activePlans に積まれる、FIMS内部の比較用飛行計画。
 * segment conflict 判定や completed時の削除に使う。
 *
 * @typedef {object} ActivePlan
 * @property {string} flightPlanId - flightPlanId
 * @property {string} who - 計画所有者（送信元UASSP）
 * @property {FlightRoute} route - ルート情報
 * @property {Date|null} tStart - 実出発時刻
 * @property {Date|null} tEnd - 到着時刻
 * @property {number} [altMin] - 最低高度
 * @property {number} [altMax] - 最高高度
 * @property {TcSchedule|null} tcSchedule - TCスケジュール
 */

/**
 * semantic validation や conflict validation で記録する1件の問題。
 *
 * @typedef {object} ValidationIssue
 * @property {string} code - 問題コード。例: "MISSING_ABS_TIME"
 * @property {"error"|"warn"} [severity] - 問題の深刻度
 * @property {string} [message] - 人間向け説明
 * @property {number} [expected] - 期待値
 * @property {number} [diff] - 差分
 * @property {number|null} [actualDepartAbsMs]
 * @property {number|null} [arrivalAbsMs]
 * @property {string|null} [flightPlanId]
 * @property {string|null} [edgeId]
 */

/**
 * semanticValidateEnvelope の戻り値。
 *
 * @typedef {object} ValidationResult
 * @property {boolean} ok - 検証成功か
 * @property {"reject"|"warn"|"quarantine"} action - 推奨処理
 * @property {string|null} [traceId] - traceId
 * @property {string|null} [type] - envelope type
 * @property {string|null} [flightPlanId] - flightPlanId
 * @property {ValidationIssue[]} issues - 検出した問題一覧
 * @property {string} [mode] - validator mode
 */

/**
 * validateConflicts の戻り値。
 *
 * @typedef {object} ConflictValidationResult
 * @property {boolean} ok - 衝突なしならtrue
 * @property {"warn"|"reject"} guardMode - guardの動作モード
 * @property {object[]} conflicts - 衝突相手一覧
 */

/**
 * trace 解決に失敗した時の簡略イベント情報。
 *
 * @typedef {object} SlimEvent
 * @property {string|undefined} source - 送信元
 * @property {string|undefined} type - 種別
 * @property {string|null} traceId - traceId
 * @property {string|null} flightPlanId - flightPlanId
 */

/**
 * semantic validator に渡すFIMS側コンテキスト。
 *
 * @typedef {object} ValidationContext
 * @property {string|null} RUN_ID - 実行ID
 * @property {number|null} T0_EPOCH_MS - 絶対時刻基準点(ms)
 * @property {Map<string, PendingAssignment>} pendingByFpId - pending一覧
 */

/**
 * REGISTERで受けるpayload。
 *
 * @typedef {object} RegisterPayload
 * @property {string} name - peer名
 */

/**
 * EVENT payload の中身。
 * tactical event や guard event の共通形。
 *
 * @typedef {object} DomainEvent
 * @property {string} [eventId] - イベントID
 * @property {number} [seq] - イベント順序番号
 * @property {string} [type] - ドメインイベント種別
 * @property {string} [severity] - 深刻度
 * @property {object} [subject] - 対象
 * @property {object} [advice] - 推奨対応
 * @property {object} [details] - 詳細
 * @property {object[]} [trace] - hop履歴
 * @property {string} [source] - 生成元
 */

const SDSP_URL = process.env.SDSP_URL || "http://localhost:5000";

// ★ 追加：Traffic Controller の URL
const TC_URL = process.env.TC_URL || "http://localhost:4100";

/**
 * SDSP に geofence チェックを問い合わせる。
 * SDSP が落ちている場合は fail-open（allowed: true）でフォールバックし
 * FIMS 全体を止めない設計にする。
 *
 * 戻り値の意味：
 *   checked: false = 「未判定」であり「安全だった」ではない
 *   noFlyHits: null = 「ヒットなし」ではなく「確認できなかった」
 *   fallback: true = SDSP障害によるフォールバック許可
 *
 * @param {FlightPlan} plan
 * @param {string|null} traceId
 * @returns {Promise<{ ok: boolean, checked: boolean, allowed: boolean,
 *                     fallback: boolean, noFlyHits: any[]|null,
 *                     reason: string|null }>}
 */
async function checkGeofenceWithSdsp(plan, traceId) {
  try {
    const res = await axios.post(
      `${SDSP_URL}/api/geofence/check`,
      { route: plan.route },
      { timeout: 3000 }
    );

    // 型崩れ防止：allowed は boolean、noFlyHits は配列であることを保証
    const allowed =
      typeof res.data?.allowed === "boolean" ? res.data.allowed : true;

    const noFlyHits = Array.isArray(res.data?.noFlyHits)
      ? res.data.noFlyHits
      : [];

    return {
      ok: true,
      checked: true,
      allowed,
      noFlyHits,
      fallback: false,
      reason: null,
    };
  } catch (e) {
    logFIMS("SDSP_ERROR", {
      traceId,
      flightPlanId: plan.flightPlanId || null,
      err: e.message,
      code: e.code || null,
    });

    return {
      ok: false,
      checked: false,
      allowed: true,
      noFlyHits: null, // null = 未確認（[] = 確認済みでヒットなし とは別物）
      fallback: true,
      reason: "SDSP_UNAVAILABLE",
    };
  }
}

/**
 * FlightPlanからTCへ /schedule を投げ、スケジュール結果を取得する。
 * route.start / route.end の座標を nearestNode で TCノードへ変換してから送る。
 *
 * @param {FlightPlan} plan - スケジュール対象の飛行計画
 * @param {string|null} traceId - この要求の追跡ID
 * @returns {Promise<TcSchedule>}
 */
// Traffic Controller に /schedule を投げて、出発・到着時刻をもらう
async function scheduleWithTc(plan, traceId) {
  const route = plan.route || {};

  // ------------------------------
  // 入力検証ヘルパー
  // 「配列であること」「長さ2であること」「両要素が有限数であること」
  // を一括で検証する。null/undefined/NaN/Infinity/文字列を全て弾く。
  // ------------------------------
  const isFiniteNum = (v) => typeof v === "number" && Number.isFinite(v);

  function isValidLonLatPair(v) {
    return (
      Array.isArray(v) &&
      v.length === 2 &&
      isFiniteNum(v[0]) &&
      isFiniteNum(v[1])
    );
  }

  const start = route.start;
  const end = route.end;

  // ------------------------------
  // 入力バリデーション
  // ※ semantic-validate.js の MISSING_ROUTE_COORDINATES でも弾くが、
  //   scheduleWithTc は独立して呼ばれる可能性があるため二重チェックする。
  // ------------------------------
  if (!isValidLonLatPair(start) || !isValidLonLatPair(end)) {
    logFIMS("TC_ROUTE_GEO_INPUT_INVALID", {
      traceId,
      flightPlanId: plan.flightPlanId || null,
      start: start ?? null,
      end: end ?? null,
      message: "route.start/end must be [lon, lat] finite numbers",
    });
    return { ok: false, reason: "TC_ROUTE_GEO_INPUT_INVALID" };
  }

  // 検証通過後に分割代入（ここに到達した時点で有限数が保証されている）
  const [startLon, startLat] = start;
  const [endLon, endLat] = end;

  // ------------------------------
  // 座標 → ノードID の導出
  // ここに到達した時点で入力は有限数が保証されているため、
  // nearestNode の失敗原因は「閾値超え（マップ外）」のみ。
  // ------------------------------
  const tcFrom = nearestNode(startLon, startLat);
  const tcTo = nearestNode(endLon, endLat);

  if (!tcFrom || !tcTo) {
    logFIMS("TC_ROUTE_GEO_MAPPING_FAILED", {
      traceId,
      flightPlanId: plan.flightPlanId || null,
      start,
      end,
      tcFrom: tcFrom ?? null,
      tcTo: tcTo ?? null,
      message:
        "nearestNode returned null: point is outside map coverage or exceeds maxDistance threshold",
    });
    return { ok: false, reason: "TC_ROUTE_GEO_MAPPING_FAILED" };
  }

  // ------------------------------
  // 導出成功ログ
  // ------------------------------
  logFIMS("TC_ROUTE_DERIVED_FROM_GEO", {
    traceId,
    flightPlanId: plan.flightPlanId || null,
    start,
    end,
    tcFrom,
    tcTo,
  });

  // 同一ノードになった場合は warn（飛行距離ゼロ）
  if (tcFrom === tcTo) {
    logFIMS("TC_ROUTE_SAME_NODE_WARN", {
      traceId,
      flightPlanId: plan.flightPlanId || null,
      tcFrom,
      tcTo,
      message: "Derived tcFrom and tcTo are the same node",
    });
  }

  // ------------------------------
  // TC へのリクエスト
  // ------------------------------
  const requestedDepartIso = route.tStart || null;
  const depart = 0;

  logFIMS("SCHEDULE_WITH_TC_IN", {
    traceId,
    flightPlanId: plan.flightPlanId || null,
    tcFrom,
    tcTo,
    depart,
    requestedDepartIso,
    requestedDepartAbsMs: null,
  });

  const flights = [
    {
      id: plan.flightPlanId || `fp-${crypto.randomUUID()}`,
      from: tcFrom,
      to: tcTo,
      depart,
      requestedDepartIso,
      routeHint: { tcFrom, tcTo, derivedFromGeo: true },
    },
  ];

  const res = await axios.post(
    `${TC_URL}/schedule`,
    { flights },
    { timeout: 5000 }
  );
  const data = res.data || {};

  if (Array.isArray(data.results)) {
    const r = data.results[0] || {};
    return {
      ...r,
      runId: data.runId ?? r.runId ?? null,
      scheduleBasis: data.scheduleBasis ?? r.scheduleBasis ?? null,
    };
  }

  return data;
}

// ------------------------------
// argv
// ------------------------------
const argv = yargs(process.argv.slice(2))
  .option("analyze", { type: "boolean", default: false })
  .option("broker", { type: "string", default: "mqtt://localhost:1883" })
  .option("pendingTtlMs", { type: "number", default: 30000 }) // ★追加: pending期限(ms)
  .help()
  .parse();

const BROKER_URL = argv.broker; //mainで使用

// ------------------------------
// paths
// ------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const LOGDIR = path.join(__dirname, "../logs");
fs.mkdirSync(LOGDIR, { recursive: true });
const LOGFILE = path.join(LOGDIR, `fims.ndjson`);

function nowISO() {
  return new Date().toISOString();
}

/**
 * FIMS の NDJSON ログへ1行書き込む。
 * ファイル書き込み失敗時はコンソールエラーのみで処理を止めない。
 *
 * @param {string} msg - ログメッセージキー（例: "FLIGHT_PLAN_ASSIGNED"）
 * @param {object} [extra={}] - 追加フィールド
 * @returns {void}
 */
function logFIMS(msg, extra = {}) {
  const entry = { ts: nowISO(), source: "FIMS", msg, ...extra };
  try {
    fs.appendFileSync(LOGFILE, JSON.stringify(entry) + "\n");
    console.log(`[FIMS LOG] ${msg}`, extra);
  } catch (e) {
    console.error("[FIMS LOG WRITE ERROR]", e.message);
  }
}
// ------------------------------
// SoT context (TC is SoT)
// ------------------------------
const RUN_ID = process.env.RUN_ID || null;
const T0_EPOCH_MS = Number(process.env.T0_EPOCH_MS || 0) || null;

/**
 * semantic validator に渡す実行コンテキストを組み立てる。
 *
 * @returns {ValidationContext}
 */
function buildValidationContext() {
  return {
    RUN_ID,
    T0_EPOCH_MS,
    pendingByFpId: pendingAssignments,
  };
}

const TRACE_MODE = process.env.TRACE_MODE || "quarantine";
// "strict" | "quarantine" | "temp"

function logTraceUnresolved(evt, reason, extra = {}) {
  const slim = {
    source: evt?.source,
    type: evt?.type,
    traceId: evt?.traceId ?? null,
    flightPlanId: extractFlightPlanId(evt),
  };
  logFIMS("TRACE_UNRESOLVED", { reason, slimEvent: slim, ...extra });
}

/**
 * さまざまな位置に入っている flightPlanId を統一ロジックで抽出する。
 *
 * @param {Envelope} evt - 対象イベント
 * @returns {string|null}
 */
// evtから flightPlanId を“必ず同じ取り方”で抽出（ブレ防止）
function extractFlightPlanId(evt) {
  return (
    evt?.subject?.flightPlanId ||
    evt?.payload?.flightPlanId ||
    evt?.flightPlanId ||
    evt?.payload?.subject?.flightPlanId ||
    null
  );
}
/**
 * envelope に traceId が無い場合、flightPlanId からキャッシュ復元する。
 * 復元不能な場合は TRACE_MODE に応じて null または temp traceId を返す。
 *
 * @param {Envelope} evt - 入力イベント
 * @returns {string|null}
 */
function ensureTraceId(evt) {
  // 1) 既にtraceIdがある
  if (evt?.traceId) return evt.traceId;

  // 2) flightPlanId から FIMSローカルキャッシュで復元する
  const fpId = extractFlightPlanId(evt);
  if (fpId) {
    const cached = traceIdByFpId.get(fpId) || null;
    if (cached) return cached;

    // キャッシュに無い＝どこかの経路で「REQUESTを見ずにEVENTだけ来た」等
    logTraceUnresolved(evt, "trace_cache_miss", { flightPlanId: fpId });

    if (TRACE_MODE === "strict") return null;
    if (TRACE_MODE === "temp") {
      const tmp = `tmp-${crypto.randomUUID()}`;
      logFIMS("TRACE_TEMP_ASSIGNED", { traceId: tmp, flightPlanId: fpId });
      // tempでもキャッシュに入れて“その後のEVENT群”は同じtmpで束ねる
      traceIdByFpId.set(fpId, tmp);
      return tmp;
    }
    // quarantine
    return null;
  }

  // 3) traceIdもflightPlanIdも無い（完全に追跡不能）
  logTraceUnresolved(evt, "no_keys");

  if (TRACE_MODE === "strict") return null;
  if (TRACE_MODE === "temp") {
    const tmp = `tmp-${crypto.randomUUID()}`;
    logFIMS("TRACE_TEMP_ASSIGNED", { traceId: tmp });
    return tmp;
  }
  return null;
}

// ------------------------------
// security
// ------------------------------
// 現状: JWT_SECRET / HMAC_SECRET を全UASSP共有
// 課題: UAASSPごとに鍵を分離し、FIMSのverifyJwtEnvelopeで
//       decoded.iss と envelope.source の一致確認を追加する
// 参考: kid（Key ID）ヘッダを使ったキーストア方式が実務的
const JWT_SECRET = process.env.JWT_SECRET || "dev-demo-secret";
const HMAC_SECRET = process.env.HMAC_SECRET || "dev-demo-hmac";

const FIMS_JWT = jwt.sign({ sub: "FIMS", role: "fims" }, JWT_SECRET, {
  expiresIn: "7d",
});

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return "[" + obj.map(stableStringify).join(",") + "]";
  const keys = Object.keys(obj).sort();
  return (
    "{" +
    keys
      .map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k]))
      .join(",") +
    "}"
  );
}

function signHmac(obj) {
  const msg = stableStringify(obj);
  const mac = crypto
    .createHmac("sha256", HMAC_SECRET)
    .update(msg)
    .digest("base64");
  return mac.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * envelope 内のJWTを検証する。
 * 検証成功時はデコード結果を envelope._user に格納する。
 *
 * @param {Envelope} envelope - 検証対象
 * @returns {boolean}
 */
function verifyJwtEnvelope(envelope) {
  const token = envelope.jwt;
  if (!token) {
    logFIMS("AUTH_FAIL", { reason: "missing jwt" });
    return false;
  }
  try {
    //envelope.user = jwt.verify(token, JWT_SECRET);
    const decoded = jwt.verify(token, JWT_SECRET); //正しい場合header/payload/signature）のpayloadをJSオブジェクトとして返す
    envelope._user = decoded; //
    return true;
  } catch (e) {
    logFIMS("AUTH_FAIL", { reason: "invalid jwt", err: e.message });
    return false;
  }
}

/**
 * envelope の HMAC-SHA256 署名を検証する。
 * sig フィールドを除いた envelope を stableStringify して比較。
 * JWT verify より先に呼ぶこと（HMAC が先のポリシー）。
 *
 * @param {Envelope} envelope - 検証対象
 * @returns {boolean} 検証成功なら true
 */
function verifyHmacEnvelope(envelope) {
  const { sig, ...unsigned } = envelope;
  if (!sig) {
    logFIMS("AUTH_FAIL", { reason: "missing hmac" });
    return false;
  }
  const expected = signHmac(unsigned); //自分でも同じ計算をして「本来あるべきsig」を作る
  if (sig !== expected) {
    logFIMS("AUTH_FAIL", { reason: "invalid hmac" });
    return false;
  }
  return true;
}

// ------------------------------
// core state
// ------------------------------
/** @type {Map<string, true>} */
const peers = new Map(); // name -> true

/** @type {ActivePlan[]} */
const activePlans = []; // conflict detector用

/** @type {Map<string, PendingAssignment>} */
const pendingAssignments = new Map();
// flightPlanId -> { traceId, requester, assignedAtMs, expireAtMs, runId, scheduleBasis, tcSchedule }
// flightPlanId -> traceId（FIMSローカルキャッシュ）

/** @type {Map<string, string>} */
const traceIdByFpId = new Map();

function parseIsoOrNull(v) {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

function parseMsToDateOrNull(ms) {
  if (typeof ms !== "number") return null;
  if (!Number.isFinite(ms)) return null;
  const d = new Date(ms);
  return isNaN(d.getTime()) ? null : d;
}

function isTimeOverlap(aStart, aEnd, bStart, bEnd) {
  if (!aStart || !aEnd || !bStart || !bEnd) return true;
  return aStart <= bEnd && bStart <= aEnd;
}
function isAltOverlap(aMin, aMax, bMin, bMax) {
  if (
    typeof aMin !== "number" ||
    typeof aMax !== "number" ||
    typeof bMin !== "number" ||
    typeof bMax !== "number"
  )
    return true;
  return aMin <= bMax && bMin <= aMax;
}

/**
 * pendingに保存されたtcScheduleと、FILEDで返ってきたtcScheduleが同一かを比較する。
 *
 * @param {TcSchedule|null|undefined} a
 * @param {TcSchedule|null|undefined} b
 * @returns {boolean}
 */
function sameTcSchedule(a, b) {
  if (!a || !b) return false;

  const keys = [
    "from",
    "to",
    "requestedDepart",
    "actualDepart",
    "arrivalTime",
    "actualDepartAbsMs",
    "arrivalAbsMs",
  ];

  for (const k of keys) {
    if (a[k] !== b[k]) return false;
  }

  // routeNodes（配列）
  const ra = Array.isArray(a.routeNodes) ? a.routeNodes : [];
  const rb = Array.isArray(b.routeNodes) ? b.routeNodes : [];
  if (ra.length !== rb.length) return false;
  for (let i = 0; i < ra.length; i++) if (ra[i] !== rb[i]) return false;

  // ★追加：segments（4-C）
  const sa = Array.isArray(a.segments) ? a.segments : [];
  const sb = Array.isArray(b.segments) ? b.segments : [];
  if (sa.length !== sb.length) return false;

  // 最小：edgeId + abs窓 だけ比較（順序も一致前提）
  for (let i = 0; i < sa.length; i++) {
    const A = sa[i] || {};
    const B = sb[i] || {};
    if ((A.edgeId || null) !== (B.edgeId || null)) return false;
    if ((A.tStartAbsMs ?? null) !== (B.tStartAbsMs ?? null)) return false;
    if ((A.tEndAbsMs ?? null) !== (B.tEndAbsMs ?? null)) return false;
  }

  return true;
}

/**
 * FIMSから外へ送る envelope を構築し、traceId / JWT / HMAC を付与する。
 *
 * @param {Envelope} baseEvt - 元になるイベント
 * @param {string|null} traceId - 強制的に載せるtraceId
 * @returns {Envelope}
 */
function buildSignedEnvelope(baseEvt, traceId) {
  // traceId は必ず上書き
  const base = { ...baseEvt, traceId };

  // ★重要：FIMS から外へ出る envelope は「常に FIMS JWT」に統一する
  // 受信した jwt を温存すると、将来 UASSP 側で JWT verify を入れた時に混在事故が起きやすい
  base.jwt = FIMS_JWT;

  // 署名対象からは sig / _user を除外（現状の方針を維持）
  const unsigned = { ...base };
  delete unsigned.sig;
  delete unsigned._user;

  const sig = signHmac(unsigned);
  return { ...unsigned, sig };
}

/**
 * PLAN_REJECTED を申請元へ通知する。
 *
 * @param {import("mqtt").MqttClient} mqttClient
 * @param {string|null} to - 通知先peer
 * @param {string|null} flightPlanId
 * @param {string|null} traceId
 * @param {string} reason - reject理由
 * @returns {void}
 */
function rejectPlan(mqttClient, to, flightPlanId, traceId, reason) {
  logFIMS("FLIGHT_PLAN_REJECTED", {
    traceId,
    flightPlanId,
    reason,
    sender: "FIMS",
    to: to || null,
  });

  const rejBody = {
    source: "FIMS",
    type: "PLAN_REJECTED",
    traceId,
    payload: { flightPlanId, reason },
    ts: Date.now(),
  };

  const rejEnvelope = buildSignedEnvelope(rejBody, traceId);
  mqttClient.publish(`utm/notify/${to}`, JSON.stringify(rejEnvelope));
}

function shareRouteNodes(a, b) {
  const ra = Array.isArray(a?.tcSchedule?.routeNodes)
    ? a.tcSchedule.routeNodes
    : [];
  const rb = Array.isArray(b?.tcSchedule?.routeNodes)
    ? b.tcSchedule.routeNodes
    : [];
  if (!ra.length || !rb.length) return false;

  const setA = new Set(ra);
  for (const n of rb) if (setA.has(n)) return true;
  return false;
}

/**
 * validation issues の中から最優先の reject reason を選ぶ。
 * hardPriority の順番が「重大度の高い順」を意味する。
 *
 * @param {ValidationIssue[]} issues
 * @returns {string} reject reason コード
 */
function pickRejectReason(issues) {
  const hardPriority = [
    "FILED_WITHOUT_PENDING",
    "FILED_AFTER_TIMEOUT",
    "FILED_TRACE_MISMATCH",
    "MISSING_FLIGHT_PLAN_ID",
    "MISSING_TC_SCHEDULE",
    "MISSING_ABS_TIME", // ★追加
    "MISSING_SEGMENTS",
    "RUN_ID_MISMATCH",
    "T0_EPOCH_MS_MISMATCH",
    "TIME_DIRECTION_INVALID",
    "ABS_TIME_ORDER_INVALID",
    "ACTUAL_DEPART_ABS_INCONSISTENT",
    "ARRIVAL_ABS_INCONSISTENT",
    "MISSING_ROUTE_NODES",
    "SEGMENTS_NOT_MONOTONIC",
    "SEGMENT_TIME_ORDER_INVALID",
    "SEGMENT_BEFORE_DEPART",
    "SEGMENT_AFTER_ARRIVAL",
  ];

  for (const code of hardPriority) {
    if (issues.some((i) => i.code === code)) return code;
  }
  return issues[0]?.code || "SEMANTIC_ERROR";
}

/**
 * peers へ通知（utm/notify/<peer>）
 * - skip: 送信元には送らない（デフォルト true）
 * - originalType: ログに残す用（デフォルト evt.type）
 * - forceType: 送信する type を上書きしたい場合だけ使う。プロトコル意味を壊す可能性あり。通常運用では使用禁止。
 */

/**
 * peer一覧へイベントを通知する。
 *
 * @param {import("mqtt").MqttClient} mqttClient
 * @param {Envelope} evt
 * @param {string|null} traceId
 * @param {{skip?: boolean, originalType?: string, forceType?: string|null}} [opts]
 * @returns {void}
 */
function notifyPeers(mqttClient, evt, traceId, opts = {}) {
  const skip = opts.skip ?? true;
  const originalType = opts.originalType ?? evt?.type ?? "(unknown)";
  const forceType = opts.forceType ?? null;

  if (!mqttClient || !evt) return;

  // flightPlanId は “同じ取り方” に統一（既に関数あるので使う）
  const fpId = extractFlightPlanId(evt);

  for (const name of peers.keys()) {
    if (skip && name === evt.source) continue;

    logFIMS("NOTIFY_OUT", {
      traceId,
      flightPlanId: fpId,
      to: name,
      originalType,
    });

    // 送信用イベント（必要なら type だけ上書き）
    const baseEvt = forceType ? { ...evt, type: forceType } : evt;

    // 署名（traceId は必ずここで上書き）
    const signedBody = buildSignedEnvelope(baseEvt, traceId);

    mqttClient.publish(`utm/notify/${name}`, JSON.stringify(signedBody));
  }
}

/**
 * pendingに保存した tcSchedule と filed の tcSchedule の
 * actualDepartAbsMs / arrivalAbsMs を比較して一致確認する。
 *
 * @param {TcSchedule|null} a - pending側
 * @param {TcSchedule|null} b - filed側
 * @returns {{ same: boolean, a: object, b: object }}
 */
function compareTcAbs(a, b) {
  const aDep = a?.actualDepartAbsMs ?? null;
  const aArr = a?.arrivalAbsMs ?? null;
  const bDep = b?.actualDepartAbsMs ?? null;
  const bArr = b?.arrivalAbsMs ?? null;

  const same =
    typeof aDep === "number" &&
    typeof aArr === "number" &&
    typeof bDep === "number" &&
    typeof bArr === "number" &&
    aDep === bDep &&
    aArr === bArr;

  return {
    same,
    a: { actualDepartAbsMs: aDep, arrivalAbsMs: aArr },
    b: { actualDepartAbsMs: bDep, arrivalAbsMs: bArr },
  };
}

// ------------------------------
// segment conflict (detailed)
// ------------------------------
const SEGMENT_MISSING_AS_CONFLICT =
  (process.env.SEGMENT_MISSING_AS_CONFLICT || "false") === "true";

// 欠損時の扱いを env で切替
function isMsOverlap(aStart, aEnd, bStart, bEnd) {
  if (
    typeof aStart !== "number" ||
    typeof aEnd !== "number" ||
    typeof bStart !== "number" ||
    typeof bEnd !== "number"
  ) {
    return SEGMENT_MISSING_AS_CONFLICT;
  }
  return aStart <= bEnd && bStart <= aEnd;
}

/**
 * segment conflict guard イベントを生成し、peerへ通知する。
 *
 * @param {import("mqtt").MqttClient} mqttClient
 * @param {string|null} traceId
 * @param {ActivePlan} newPlan
 * @param {object[]} conflicts
 * @returns {void}
 */
function emitGuardEventSegmentConflict(
  mqttClient,
  traceId,
  newPlan,
  conflicts
) {
  if (!mqttClient) return;

  const nowMs = Date.now();

  const evt = {
    eventId: `evt-${nowMs}-${crypto.randomUUID()}`,
    seq: 0, // ガード用途なので固定でOK（必要なら後で運用ルール化）
    type: "SEGMENT_CONFLICT_GUARD",
    severity: "HIGH",
    subject: {
      flightPlanId: newPlan.flightPlanId,
      who: newPlan.who,
    },
    advice: {
      action: "INVESTIGATE",
      reason: "segments_overlap_detected_in_fims_guard",
    },
    details: {
      runId: newPlan?.tcSchedule?.runId ?? null,
      scheduleBasis: newPlan?.tcSchedule?.scheduleBasis ?? null,
      tcAbs: {
        actualDepartAbsMs: newPlan?.tcSchedule?.actualDepartAbsMs ?? null,
        arrivalAbsMs: newPlan?.tcSchedule?.arrivalAbsMs ?? null,
      },
      conflictsWith: conflicts,
    },
    trace: [{ hop: "FIMS_GUARD", name: "FIMS", t_detect: nowISO() }],
    source: "FIMS",
  };

  const body = {
    source: "FIMS",
    type: "EVENT",
    traceId,
    payload: evt,
    ts: nowMs,
  };

  // peersへ通知（FIMSがsourceなので全peerに飛ぶ）
  notifyPeers(mqttClient, body, traceId, { originalType: "EVENT" });

  logFIMS("GUARD_EVENT_EMITTED", {
    traceId,
    flightPlanId: newPlan.flightPlanId,
    eventType: evt.type,
    conflictsCount: Array.isArray(conflicts) ? conflicts.length : null,
  });
}

// ------------------------------
// handlers
// ------------------------------
/**
 * REGISTERメッセージを処理し、peer一覧へ登録する。
 *
 * @param {Envelope & { payload?: RegisterPayload }} envelope
 * @returns {void}
 */
function handleRegister(envelope) {
  if (!verifyHmacEnvelope(envelope)) return; // ★先にHMAC
  if (!verifyJwtEnvelope(envelope)) return; // ★後でJWT

  const { payload } = envelope || {};
  const { name } = payload || {};
  if (!name) return;

  peers.set(name, true);
  logFIMS("REGISTER_IN", { who: name });
  console.log("[FIMS] peers now:", [...peers.keys()]);
}
/**
 * utm/events で受けたイベントを種別ごとに処理する。
 *
 * @param {Envelope} envelope
 * @param {import("mqtt").MqttClient} mqttClient
 * @returns {Promise<void>}
 */
async function handleEvent(envelope, mqttClient) {
  // まず認証
  if (!verifyHmacEnvelope(envelope)) return; // ★先にHMAC
  if (!verifyJwtEnvelope(envelope)) return; // ★後でJWT

  const evt = envelope;
  const { source, type, ts } = evt || {};
  if (!source || !type) return;

  if (ts == null) {
    logFIMS("DROP_EVENT_NO_TS", {
      source,
      type,
      flightPlanId: extractFlightPlanId(evt),
    });
    return;
  }

  const traceId = ensureTraceId(evt);

  if (!traceId) {
    // ensureTraceId側でも TRACE_UNRESOLVED は出るが、
    // ここで「FIMSが処理を打ち切った」ことを明確に残す
    logFIMS("DROP_EVENT_NO_TRACE", {
      source: evt?.source || null,
      type: evt?.type || null,
      flightPlanId: extractFlightPlanId(evt),
      mode: TRACE_MODE,
    });
    return;
  }

  evt.traceId = traceId;

  // ------------------------------
  // 1) FLIGHT_PLAN_REQUEST
  // ------------------------------
  if (evt.type === "FLIGHT_PLAN_REQUEST" && evt.payload) {
    const plan = evt.payload;
    // ★ semantic validation (AJV後の意味チェック)
    const ctx = buildValidationContext();
    const v = semanticValidateEnvelope(evt, ctx);

    if (!v.ok) {
      logFIMS("SEMANTIC_VALIDATION_FAILED", {
        traceId,
        flightPlanId: plan.flightPlanId || null,
        action: v.action,
        issues: v.issues,
      });

      if (v.action === "reject") {
        const reason = pickRejectReason(v.issues);
        return rejectPlan(
          mqttClient,
          evt.source,
          plan.flightPlanId,
          traceId,
          reason
        );
      }

      if (v.action === "quarantine") {
        logFIMS("REQUEST_QUARANTINED", {
          traceId,
          flightPlanId: plan.flightPlanId || null,
          issues: v.issues,
        });
        return;
      }
      // warnは続行
    }

    const route = plan.route || {};

    // ★削除：newPlan は不要
    // const newPlan = { ... };
    // newPlan.tcSchedule = { ... };

    // traceId キャッシュ
    if (plan?.flightPlanId && traceId) {
      traceIdByFpId.set(plan.flightPlanId, traceId);
      logFIMS("TRACE_CACHE_SET", { traceId, flightPlanId: plan.flightPlanId });
    }

    // SDSP Geofence チェック
    const sdsp = await checkGeofenceWithSdsp(plan, traceId);

    // checked: false = SDSPが落ちていて未判定だったことを明示的に残す
    // allowed: true と checked: true を混同しないように別イベントで分ける
    // → analyzeLog で SDSP障害回数 / fail-open発生回数 を独立集計できる
    if (!sdsp.checked) {
      logFIMS("GEOFENCE_FALLBACK_ALLOW", {
        traceId,
        flightPlanId: plan.flightPlanId || null,
        reason: sdsp.reason,
      });
    }

    logFIMS("SDSP_GEOFENCE_RESULT", {
      traceId,
      flightPlanId: plan.flightPlanId || null,
      sender: evt.source,
      ok: sdsp.ok,
      checked: sdsp.checked, // 追加：業務判定が成立したか
      allowed: sdsp.allowed,
      fallback: sdsp.fallback, // 追加：フォールバック許可か
      noFlyHits: sdsp.noFlyHits, // null=未確認 / []=確認済みでヒットなし
      reason: sdsp.reason ?? null,
    });

    // Geofence NG → reject
    if (!sdsp.allowed) {
      return rejectPlan(
        mqttClient,
        evt.source,
        plan.flightPlanId,
        traceId,
        "GEOFENCE_BLOCKED"
      );
    }

    // TC スケジュール

    let tc;
    try {
      tc = await scheduleWithTc(plan, traceId);
    } catch (e) {
      logFIMS("TC_ERROR", {
        traceId,
        flightPlanId: plan.flightPlanId,
        err: e.message,
        code: e.code || null,
      });
      return rejectPlan(
        mqttClient,
        evt.source,
        plan.flightPlanId,
        traceId,
        "TC_ERROR"
      );
    }

    if (!tc || !tc.ok) {
      // scheduleWithTc が reason を持つ場合はそのまま使う（GEO系の失敗）
      // 持たない場合（TC本体の拒否）は TC_REJECT にフォールバック
      const reason = tc?.reason || "TC_REJECT";
      logFIMS("TC_SCHEDULE_FAILED", {
        traceId,
        flightPlanId: plan.flightPlanId,
        reason,
      });
      return rejectPlan(
        mqttClient,
        evt.source,
        plan.flightPlanId,
        traceId,
        reason
      );
    }

    // ASSIGNED 送信
    const accBody = {
      source: "FIMS",
      type: "FLIGHT_PLAN_ASSIGNED",
      traceId,
      payload: {
        flightPlanId: plan.flightPlanId,
        runId: tc.runId || null,
        scheduleBasis: tc.scheduleBasis || null,
        tcSchedule: {
          from: tc.from,
          to: tc.to,
          requestedDepart: tc.requestedDepart,
          actualDepart: tc.actualDepart,
          arrivalTime: tc.arrivalTime,
          routeNodes: tc.routeNodes,
          actualDepartAbsMs: tc.actualDepartAbsMs ?? null,
          arrivalAbsMs: tc.arrivalAbsMs ?? null,
          segments: Array.isArray(tc.segments) ? tc.segments : [],
        },
      },
      ts: Date.now(),
    };
    const accEnvelope = buildSignedEnvelope(accBody, traceId);

    mqttClient.publish(`utm/notify/${evt.source}`, JSON.stringify(accEnvelope));

    logFIMS("FLIGHT_PLAN_ASSIGNED", {
      traceId,
      flightPlanId: plan.flightPlanId,
      to: evt.source,
      forwarded: true,
      runId: accBody.payload.runId || null,
      scheduleBasis: accBody.payload.scheduleBasis || null,
      tcRel: {
        actualDepart: accBody.payload.tcSchedule?.actualDepart ?? null,
        arrivalTime: accBody.payload.tcSchedule?.arrivalTime ?? null,
      },
      tcAbs: {
        actualDepartAbsMs:
          accBody.payload.tcSchedule?.actualDepartAbsMs ?? null,
        arrivalAbsMs: accBody.payload.tcSchedule?.arrivalAbsMs ?? null,
      },
      routeNodes: Array.isArray(accBody.payload.tcSchedule?.routeNodes)
        ? accBody.payload.tcSchedule.routeNodes
        : null,
    });

    // pending に登録
    pendingAssignments.set(plan.flightPlanId, {
      traceId,
      requester: evt.source, // ★SoT: 申請元（送り元）
      assignedAtMs: Date.now(),
      expireAtMs: Date.now() + Math.max(0, Number(argv.pendingTtlMs) || 0),
      runId: accBody.payload.runId,
      scheduleBasis: accBody.payload.scheduleBasis,
      tcSchedule: accBody.payload.tcSchedule,
    });

    logFIMS("PENDING_ASSIGNED_SET", {
      traceId,
      flightPlanId: plan.flightPlanId,
      requester: evt.source,
    });

    return;
  }
  // ------------------------------
  // 2) FLIGHT_PLAN_FILED（最終確定）
  // ------------------------------
  if (evt.type === "FLIGHT_PLAN_FILED" && evt.payload) {
    const plan = evt.payload;
    const route = plan.route || {};
    const fpId = plan.flightPlanId || "(unknown)";

    // ★ semantic validation (FILEDは強めに止める)
    const ctx = buildValidationContext();
    const v = semanticValidateEnvelope(evt, ctx);

    if (!v.ok) {
      logFIMS("SEMANTIC_VALIDATION_FAILED", {
        traceId,
        flightPlanId: fpId,
        action: v.action,
        issues: v.issues,
      });

      const hardRejectCodes = new Set([
        "FILED_WITHOUT_PENDING",
        "FILED_AFTER_TIMEOUT",
        "FILED_TRACE_MISMATCH",
        "MISSING_FLIGHT_PLAN_ID",
        "MISSING_TC_SCHEDULE",
        "MISSING_SEGMENTS",
        // ★追加（透明化対策の要）
        "MISSING_ABS_TIME",
        "MISSING_RELATIVE_TIME_FIELDS",
        "MISSING_ROUTE_NODES",
        "SEGMENT_TIME_MISSING",
        "SEGMENT_EDGE_INVALID",
        "SEGMENT_TIME_ORDER_INVALID",
        "SEGMENT_BEFORE_DEPART",
        "SEGMENT_AFTER_ARRIVAL",
        "SEGMENTS_NOT_MONOTONIC",

        "RUN_ID_MISMATCH",
        "T0_EPOCH_MS_MISMATCH",
        "TIME_DIRECTION_INVALID",
        "ABS_TIME_ORDER_INVALID",
      ]);

      const hasHard = v.issues.some((x) => hardRejectCodes.has(x.code));

      if (v.action === "reject" || hasHard) {
        const reason = pickRejectReason(v.issues);
        return rejectPlan(mqttClient, evt.source, fpId, traceId, reason);
      }

      if (v.action === "quarantine") {
        logFIMS("FILED_QUARANTINED", {
          traceId,
          flightPlanId: fpId,
          issues: v.issues,
        });
        return;
      }
      // warnは続行
    }

    if (fpId && traceId) {
      if (!traceIdByFpId.has(fpId)) {
        traceIdByFpId.set(fpId, traceId);
        logFIMS("TRACE_CACHE_SET_BY_FILED", { traceId, flightPlanId: fpId });
      }
    }

    // ★ まず pending を探す（ASSIGNED→FILED の整合チェック）
    const pending = pendingAssignments.get(fpId) || null;

    if (!pending) {
      return rejectPlan(
        mqttClient,
        evt.source,
        fpId,
        traceId,
        "FILED_WITHOUT_PENDING"
      );
    } else {
      // ========================================
      // 【改善4】FILED時のタイムアウトチェック
      // ========================================
      const now = Date.now();
      const expireAtMs = pending.expireAtMs ?? null;

      if (
        typeof expireAtMs === "number" &&
        expireAtMs > 0 &&
        now > expireAtMs
      ) {
        // ★ pending期限切れのFILEDを拒否
        const ageMs = pending.assignedAtMs ? now - pending.assignedAtMs : null;
        const ttlMs =
          pending.assignedAtMs && expireAtMs
            ? expireAtMs - pending.assignedAtMs
            : null;

        logFIMS("FILED_AFTER_TIMEOUT", {
          traceId,
          flightPlanId: fpId,
          requester: pending.requester || null,
          sender: evt.source || null, // ★FILED送信者も欲しければ残す（任意）//requester（申請者）」と「sender（今回送った人）」
          ageMs,
          ttlMs,
          expireAtMs,
          nowMs: now,
          overMs: now - expireAtMs, // どれだけ遅れたか
        });

        // ★ pending削除（sweep と同じ）
        pendingAssignments.delete(fpId);

        // ★ このFILEDは処理せず終了
        return rejectPlan(
          mqttClient,
          evt.source,
          fpId,
          traceId,
          "FILED_AFTER_TIMEOUT"
        );
      }
      // ========================================
      // 【改善4 終わり】以下は既存コード
      // ========================================
      // traceId 一致チェック
      if (pending.traceId !== traceId) {
        logFIMS("FILED_TRACE_MISMATCH", {
          flightPlanId: fpId,
          requester: pending.requester || null,
          sender: evt.source || null,
          pendingTraceId: pending.traceId,
          filedTraceId: traceId,
        });
        // 厳密運用ならここで reject も可（まずはログだけ推奨）
      }

      // tcSchedule 一致チェック（UASSPが payload に載せて返してくる想定）
      const filedTc = plan.tcSchedule || null;
      if (!filedTc) {
        logFIMS("FILED_MISSING_TCSCHEDULE", {
          traceId,
          flightPlanId: fpId,
          sender: evt.source || null,
          requester: pending?.requester ?? null,
        });
      } else if (!sameTcSchedule(pending.tcSchedule, filedTc)) {
        logFIMS("FILED_TCSCHEDULE_MISMATCH", {
          traceId,
          flightPlanId: fpId,
          requester: pending.requester || null,
          sender: evt.source || null,
        });
        // 必要ならデバッグ用に差分を出す（まずは重くしないため省略）
      }

      // ここまで来たら pending は消す（確定したので）
      pendingAssignments.delete(fpId);
      logFIMS("PENDING_ASSIGNED_CLEARED", {
        traceId,
        flightPlanId: fpId,
        requester: pending.requester || null,
      });
    }
    //

    const filedTc = plan.tcSchedule || null;

    const newPlan = {
      flightPlanId: fpId,
      who: evt.source || "(unknown UASSP)",
      route,

      // ★SoT：TCの絶対時刻
      tStart: parseMsToDateOrNull(filedTc?.actualDepartAbsMs),
      tEnd: parseMsToDateOrNull(filedTc?.arrivalAbsMs),

      altMin: route.altMin,
      altMax: route.altMax,
      tcSchedule: filedTc,
      expireAtMs: filedTc?.arrivalAbsMs ?? Date.now() + 3600000, // TCの到着時刻を期限とする
    };

    const pendingTc = pending?.tcSchedule || null;

    const absCmp = compareTcAbs(pendingTc, filedTc);

    logFIMS("FLIGHT_PLAN_FILED_IN", {
      traceId,
      flightPlanId: fpId,
      sender: evt.source,
      requester: pending?.requester ?? null, //sender と requester が同じでも 両方書く

      runId: plan.runId || pending?.runId || null,
      scheduleBasis: plan.scheduleBasis || pending?.scheduleBasis || null,

      tcRel: {
        actualDepart: filedTc?.actualDepart ?? null,
        arrivalTime: filedTc?.arrivalTime ?? null,
      },

      tcAbs: {
        actualDepartAbsMs: filedTc?.actualDepartAbsMs ?? null,
        arrivalAbsMs: filedTc?.arrivalAbsMs ?? null,
      },

      pendingTcAbs: absCmp.a,
      filedTcAbs: absCmp.b,
      absMatch: absCmp.same,

      routeNodes: Array.isArray(filedTc?.routeNodes)
        ? filedTc.routeNodes
        : null,
    });

    const SEGMENT_GUARD_MODE = process.env.SEGMENT_GUARD_MODE || "warn";

    const conflictVal = validateConflicts(newPlan, activePlans, {
      guardMode: SEGMENT_GUARD_MODE,
      missingAsConflict: SEGMENT_MISSING_AS_CONFLICT,
      conflictLimit: 10,
    });

    if (!conflictVal.ok) {
      const msg =
        conflictVal.guardMode === "reject"
          ? "FILED_REJECTED_SEGMENT_CONFLICT"
          : "FILED_SEGMENT_CONFLICT_WARN";

      logFIMS(msg, {
        traceId,
        flightPlanId: newPlan.flightPlanId,
        who: newPlan.who,
        conflictsWith: conflictVal.conflicts,
        mode: conflictVal.guardMode,
      });

      const SEGMENT_GUARD_EMIT_EVENT =
        (process.env.SEGMENT_GUARD_EMIT_EVENT || "true") === "true";

      if (SEGMENT_GUARD_EMIT_EVENT) {
        emitGuardEventSegmentConflict(
          mqttClient,
          traceId,
          newPlan,
          conflictVal.conflicts
        );
      }

      if (conflictVal.guardMode === "reject") {
        return rejectPlan(
          mqttClient,
          evt.source,
          newPlan.flightPlanId,
          traceId,
          "SEGMENT_CONFLICT_GUARD"
        );
      }
    }

    // tcSchedule がある plan だけ activePlans に積む
    activePlans.push(newPlan);

    logFIMS("ACTIVE_ADD", {
      traceId,
      flightPlanId: newPlan.flightPlanId,
      owner: newPlan.who,

      runId: plan.runId || pending?.runId || null,
      scheduleBasis: plan.scheduleBasis || pending?.scheduleBasis || null,

      tcAbs: {
        actualDepartAbsMs: newPlan.tcSchedule?.actualDepartAbsMs ?? null,
        arrivalAbsMs: newPlan.tcSchedule?.arrivalAbsMs ?? null,
      },

      routeNodes: Array.isArray(newPlan.tcSchedule?.routeNodes)
        ? newPlan.tcSchedule.routeNodes
        : null,
    });

    notifyPeers(mqttClient, evt, traceId, {
      originalType: "FLIGHT_PLAN_FILED",
    });

    return;
  }

  // ------------------------------
  // 3) EVENT 系（TACTICAL_DECONFLICT 等）
  // ------------------------------
  if (evt.type === "EVENT") {
    notifyPeers(mqttClient, evt, traceId, { originalType: "EVENT" });
    return;
  }
  // ------------------------------
  // 4) FLIGHT_PLAN_COMPLETED（正常終了）
  // ------------------------------
  if (evt.type === "FLIGHT_PLAN_COMPLETED" && evt.payload) {
    const fpId = evt.payload.flightPlanId || evt.flightPlanId || "(unknown)";

    // pending が残ってたら消す（本来はFILED後に来る想定だが保険）
    if (pendingAssignments.has(fpId)) {
      pendingAssignments.delete(fpId);
      logFIMS("PENDING_CLEARED_BY_COMPLETED", { traceId, flightPlanId: fpId });
    }

    // activePlans から削除
    const before = activePlans.length;
    for (let i = activePlans.length - 1; i >= 0; i--) {
      if (activePlans[i]?.flightPlanId === fpId) activePlans.splice(i, 1);
    }
    const after = activePlans.length;

    logFIMS("FLIGHT_PLAN_COMPLETED_IN", {
      traceId,
      flightPlanId: fpId,
      sender: evt.source || null,
      removed: before - after,
      activeNow: after,
    });

    if (fpId && traceIdByFpId.has(fpId)) {
      traceIdByFpId.delete(fpId);
      logFIMS("TRACE_CACHE_CLEARED", { traceId, flightPlanId: fpId });
    }

    // ★ peers に COMPLETED を通知（申請元以外）※ notifyPeers に統一
    // evt はこの時点で traceId が入っているので、そのまま渡してOK
    notifyPeers(mqttClient, evt, traceId, {
      originalType: "FLIGHT_PLAN_COMPLETED",
    });
    return;
  }
}
/**
 * 期限切れpendingを掃除し、必要に応じて timeout reject を返す。
 *
 * @param {import("mqtt").MqttClient} mqttClient
 * @returns {void}
 */
function sweepPendingAssignments(mqttClient) {
  const now = Date.now();
  const expired = [];

  for (const [flightPlanId, p] of pendingAssignments.entries()) {
    const exp = p?.expireAtMs ?? null; //expireAtMs: Date.now() + ...pendingAssignments.set()で作成済み
    if (typeof exp === "number" && exp > 0 && now >= exp) {
      expired.push({ flightPlanId, p });
    }
  }
  //`p === pending record`pendingAssignments.setされたもの
  for (const e of expired) {
    // ★追加：FILEDが先に確定して pending が消えていたら、この期限処理は中止する
    if (!pendingAssignments.has(e.flightPlanId)) {
      logFIMS("PENDING_EXPIRE_SKIPPED_ALREADY_CLEARED", {
        traceId: e.p?.traceId ?? null,
        flightPlanId: e.flightPlanId,
        reason: "cleared_by_filed_or_completed",
      });
      continue;
    }

    const p = e.p || {}; //plan抽出

    const traceId = p.traceId || null;
    const requester = p.requester || null;

    // まず期限切れログ
    logFIMS("PENDING_ASSIGNED_EXPIRED", {
      traceId,
      flightPlanId: e.flightPlanId,
      requester,
      ageMs: p.assignedAtMs ? now - p.assignedAtMs : null,
      ttlMs:
        p.assignedAtMs && p.expireAtMs ? p.expireAtMs - p.assignedAtMs : null,
    });

    if (!traceId) {
      logFIMS("REJECT_WITHOUT_TRACE", {
        flightPlanId: e.flightPlanId,
        reason: "FILED_TIMEOUT",
      });
      pendingAssignments.delete(e.flightPlanId); // ★掃除はする
      continue; // ★他も処理する
    }

    // ★ timeout reject を申請元へ返す（requester が分かる場合）
    if (requester && mqttClient) {
      const rejBody = {
        source: "FIMS",
        type: "PLAN_REJECTED",
        traceId: traceId,
        payload: {
          flightPlanId: e.flightPlanId,
          reason: "FILED_TIMEOUT",
        },
        ts: Date.now(),
      };
      const rejEnvelope = buildSignedEnvelope(rejBody, traceId);

      mqttClient.publish(
        `utm/notify/${requester}`,
        JSON.stringify(rejEnvelope)
      );

      logFIMS("PENDING_ASSIGNED_TIMEOUT_REJECTED", {
        traceId: rejBody.traceId,
        flightPlanId: e.flightPlanId,
        to: requester,
      });
    } else {
      logFIMS("PENDING_ASSIGNED_TIMEOUT_REJECT_SKIPPED", {
        traceId,
        flightPlanId: e.flightPlanId,
        reason: requester ? "missing_mqttClient" : "missing_requester",
      });
    }

    // pending を消す（timeout確定）
    pendingAssignments.delete(e.flightPlanId);
  }
}

/**
 * 到着時刻（arrivalAbsMs）を過ぎた activePlan を掃除する。
 * COMPLETEDが来ない場合（クラッシュ・実験中断）への保険。
 * sweepPendingAssignments と同じ setInterval から呼ぶ。
 *
 * @returns {void}
 */
function sweepActivePlans() {
  const now = Date.now();
  for (let i = activePlans.length - 1; i >= 0; i--) {
    const p = activePlans[i];
    if (p.expireAtMs && now > p.expireAtMs) {
      logFIMS("ACTIVE_PLAN_EXPIRED_TTL", {
        flightPlanId: p.flightPlanId,
        who: p.who,
        expireAtMs: p.expireAtMs,
        activeNow: activePlans.length - 1,
      });
      activePlans.splice(i, 1);
    }
  }
}

console.log("[FIMS] HMAC_SECRET =", HMAC_SECRET);

// ------------------------------
// MQTT main
// ------------------------------
async function main() {
  if (argv.analyze) {
    //--analyze ならサーバは起動せず、ログ解析して終了
    await analyzeLog();
    process.exit(0);
  }

  const mqttClient = mqtt.connect(BROKER_URL, {
    clientId: "FIMS",
    reconnectPeriod: 1000,
  });

  mqttClient.on("connect", () => {
    console.log(`[MQTT] FIMS connected ${BROKER_URL}`);
    mqttClient.subscribe("utm/register");
    mqttClient.subscribe("utm/events");
  });

  // ★ pending の期限切れ掃除（5秒ごと）
  setInterval(() => {
    try {
      sweepPendingAssignments(mqttClient);
      sweepActivePlans(); // ★追加
    } catch (e) {
      logFIMS("PENDING_SWEEP_ERROR", { err: e.message });
    }
  }, 5000);

  mqttClient.on("message", async (topic, msgBuf) => {
    let envelope;
    try {
      envelope = JSON.parse(msgBuf.toString("utf8")) || {};
    } catch (e) {
      logFIMS("MSG_PARSE_FAIL", { topic, err: e.message });
      return;
    }

    if (topic === "utm/register") handleRegister(envelope);
    if (topic === "utm/events") await handleEvent(envelope, mqttClient);
  });

  mqttClient.on("error", (e) => console.error("[MQTT ERROR]", e.message));
}

main().catch((e) => {
  console.error("[FATAL] FIMS main failed:", e);
  process.exit(1);
});

// ------------------------------
// analyzer (元のまま)
// ------------------------------
async function analyzeLog() {
  const rl = readline.createInterface({
    input: fs.createReadStream(LOGFILE, "utf8"),
    crlfDelay: Infinity,
  });

  const countsByMsg = {};
  let firstTs = null;
  let lastTs = null;
  const byPlan = new Map();
  const get = (k) => countsByMsg[k] || 0;

  // ★ 問題サンプルを拾う（最大10件ずつ）
  const bad = {
    pendingExpired: [],
    pendingTimeoutRejected: [],
    filedWithoutPending: [],
    filedTraceMismatch: [],
    filedTcMismatch: [],
    sdspFallback: [], // ★追加：SDSPフォールバック発生サンプル
    sdspError: [], // ★追加：SDSP通信エラーサンプル
    geofenceBlocked: [], // ★追加：Geofence拒否サンプル
  };
  const pushLimited = (arr, item, limit = 10) => {
    if (arr.length < limit) arr.push(item);
  };

  for await (const line of rl) {
    if (!line.trim()) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch {
      continue;
    }

    countsByMsg[obj.msg] = (countsByMsg[obj.msg] || 0) + 1;

    // ★ 異常サンプルの収集（最大10件）
    if (obj.msg === "PENDING_ASSIGNED_EXPIRED") {
      pushLimited(bad.pendingExpired, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        traceId: obj.traceId || null,
        requester: obj.requester || null,
        ageMs: obj.ageMs ?? null,
      });
    }
    if (obj.msg === "PENDING_ASSIGNED_TIMEOUT_REJECTED") {
      pushLimited(bad.pendingTimeoutRejected, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        traceId: obj.traceId || null,
        to: obj.to || null,
      });
    }
    // ★ FILED without pending はログ名が揺れやすいので正規化して拾う
    //   - 旧: FILED_WITHOUT_PENDING
    //   - 現: FILED_WITHOUT_PENDING_REJECTED
    //   - 将来: FILED_WITHOUT_PENDING_WARN などが増えてもここで吸収できる
    const isFiledWithoutPending =
      obj.msg === "FILED_WITHOUT_PENDING" ||
      obj.msg === "FILED_WITHOUT_PENDING_REJECTED" ||
      obj.msg === "FILED_WITHOUT_PENDING_WARN";

    if (isFiledWithoutPending) {
      pushLimited(bad.filedWithoutPending, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        traceId: obj.traceId || null,
        requester: obj.requester || null,
        // どのmsgで拾ったか残す（原因追跡が楽）
        msg: obj.msg,
      });
    }
    if (obj.msg === "FILED_TRACE_MISMATCH") {
      pushLimited(bad.filedTraceMismatch, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        pendingTraceId: obj.pendingTraceId || null,
        filedTraceId: obj.filedTraceId || null,
        requester: obj.requester || null,
      });
    }
    if (obj.msg === "FILED_TCSCHEDULE_MISMATCH") {
      pushLimited(bad.filedTcMismatch, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        traceId: obj.traceId || null,
        requester: obj.requester || null,
      });
    }
    if (obj.msg === "GEOFENCE_FALLBACK_ALLOW") {
      pushLimited(bad.sdspFallback, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        traceId: obj.traceId || null,
        reason: obj.reason || null,
      });
    }

    if (obj.msg === "SDSP_ERROR") {
      pushLimited(bad.sdspError, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        traceId: obj.traceId || null,
        err: obj.err || null,
        code: obj.code || null,
      });
    }

    if (
      obj.msg === "FLIGHT_PLAN_REJECTED" &&
      obj.reason === "GEOFENCE_BLOCKED"
    ) {
      pushLimited(bad.geofenceBlocked, {
        flightPlanId: obj.flightPlanId || "(unknown)",
        traceId: obj.traceId || null,
        to: obj.to || null,
      });
    }

    const t = new Date(obj.ts).getTime();
    if (!firstTs || t < firstTs) firstTs = t;
    if (!lastTs || t > lastTs) lastTs = t;

    const flightPlanId = obj.flightPlanId || "(unknown)";
    if (flightPlanId === "(unknown)") continue;

    if (!byPlan.has(flightPlanId)) {
      byPlan.set(flightPlanId, {
        flightPlanId,
        traceId: obj.traceId || null,
        tFiledIn: null,
        tNotifyOut: null,
        notifyToSet: new Set(),
      });
    }

    const rec = byPlan.get(flightPlanId);

    if (obj.msg === "FLIGHT_PLAN_FILED_IN") {
      rec.tFiledIn = t; //const t = new Date(obj.ts).getTime();
      if (obj.traceId) rec.traceId = obj.traceId;
    }

    if (obj.msg === "NOTIFY_OUT") {
      const isSlaNotify = obj.originalType === "FLIGHT_PLAN_FILED";
      if (!isSlaNotify) continue;
      if (rec.tFiledIn != null && t < rec.tFiledIn) continue;
      if (rec.tNotifyOut == null) rec.tNotifyOut = t;
      if (obj.to) rec.notifyToSet.add(obj.to);
      if (obj.traceId) rec.traceId = obj.traceId;
    }
  }

  const samples = [];
  const incomplete = [];

  for (const rec of byPlan.values()) {
    if (rec.tFiledIn != null && rec.tNotifyOut != null) {
      samples.push({
        flightPlanId: rec.flightPlanId,
        traceId: rec.traceId || "(no-trace)",
        delayMs: rec.tNotifyOut - rec.tFiledIn,
        notifyPeers: [...rec.notifyToSet],
      });
    } else {
      incomplete.push(rec);
    }
  }

  console.log("=== FIMS SLA ANALYZE ===");
  if (firstTs && lastTs) {
    console.log(
      "期間:",
      new Date(firstTs).toISOString(),
      "〜",
      new Date(lastTs).toISOString()
    );
  }
  console.log("メッセージ別件数:", countsByMsg);
  console.log("flightPlan総数:", byPlan.size);
  console.log("有効サンプル数:", samples.length);
  console.log("不完全(plan片側欠損):", incomplete.length);

  // ------------------------------
  // Health / Integrity summary
  // ------------------------------
  console.log("\n=== HEALTH SUMMARY ===");
  console.log("PENDING set          :", get("PENDING_ASSIGNED_SET"));
  console.log("PENDING cleared      :", get("PENDING_ASSIGNED_CLEARED"));
  console.log("PENDING expired      :", get("PENDING_ASSIGNED_EXPIRED"));
  console.log(
    "PENDING timeout reject:",
    get("PENDING_ASSIGNED_TIMEOUT_REJECTED")
  );

  console.log("COMPLETED in         :", get("FLIGHT_PLAN_COMPLETED_IN"));

  console.log("COMPLETED in         :", get("FLIGHT_PLAN_COMPLETED_IN"));

  // ★ 集計も正規化（ログ名の揺れを吸収）
  const filedWithoutPending =
    get("FILED_WITHOUT_PENDING") +
    get("FILED_WITHOUT_PENDING_REJECTED") +
    get("FILED_WITHOUT_PENDING_WARN");

  console.log("FILED without pending:", filedWithoutPending);

  console.log("FILED trace mismatch :", get("FILED_TRACE_MISMATCH"));
  console.log("FILED tc mismatch    :", get("FILED_TCSCHEDULE_MISMATCH"));
  console.log("FILED missing tc     :", get("FILED_MISSING_TCSCHEDULE"));

  const pendingSet = get("PENDING_ASSIGNED_SET");
  const pendingExpired = get("PENDING_ASSIGNED_EXPIRED");
  const pendingTimeoutRejected = get("PENDING_ASSIGNED_TIMEOUT_REJECTED");
  const pct = (a, b) => (b ? `${Math.round((a / b) * 1000) / 10}%` : "n/a");

  console.log("\n--- Rates ---");
  console.log(
    "expired / pendingSet :",
    pendingExpired,
    "/",
    pendingSet,
    "=",
    pct(pendingExpired, pendingSet)
  );
  console.log(
    "timeoutReject / pendingSet :",
    pendingTimeoutRejected,
    "/",
    pendingSet,
    "=",
    pct(pendingTimeoutRejected, pendingSet)
  );
  // ------------------------------

  // ------------------------------
  // SDSP / Geofence サマリ
  // ------------------------------
  const sdspChecked = get("SDSP_GEOFENCE_RESULT"); // SDSP問い合わせ総数
  const sdspFallback = get("GEOFENCE_FALLBACK_ALLOW"); // フォールバック発生数
  const sdspError = get("SDSP_ERROR"); // SDSP通信エラー数
  const sdspBlocked = get("FLIGHT_PLAN_REJECTED"); // ※ reason=GEOFENCE_BLOCKED のみを正確に集計するには
  //    別途 bad.geofenceBlocked.length を使う
  console.log("SDSP checked         :", sdspChecked);
  console.log(
    "SDSP fallback        :",
    sdspFallback,
    `(${
      sdspChecked ? Math.round((sdspFallback / sdspChecked) * 1000) / 10 : "n/a"
    }%)`
  );
  console.log("SDSP error           :", sdspError);
  console.log("Geofence blocked     :", bad.geofenceBlocked.length);

  // ------------------------------
  // Problem samples (max 10 each)
  // ------------------------------
  const printList = (title, arr) => {
    console.log(`\n--- ${title} (showing ${arr.length}) ---`);
    if (!arr.length) return;
    for (const x of arr) {
      const parts = [];
      if (x.flightPlanId) parts.push(`flightPlanId=${x.flightPlanId}`);
      if (x.msg) parts.push(`msg=${x.msg}`);
      if (x.traceId) parts.push(`traceId=${x.traceId}`);
      if (x.requester) parts.push(`requester=${x.requester}`);
      if (x.to) parts.push(`to=${x.to}`);
      if (x.ageMs != null) parts.push(`ageMs=${x.ageMs}`);
      if (x.pendingTraceId) parts.push(`pendingTraceId=${x.pendingTraceId}`);
      if (x.filedTraceId) parts.push(`filedTraceId=${x.filedTraceId}`);
      console.log("  -", parts.join(" "));
    }
  };

  printList("PENDING_ASSIGNED_EXPIRED", bad.pendingExpired);
  printList("PENDING_ASSIGNED_TIMEOUT_REJECTED", bad.pendingTimeoutRejected);
  printList("FILED_WITHOUT_PENDING (normalized)", bad.filedWithoutPending);
  printList("FILED_TRACE_MISMATCH", bad.filedTraceMismatch);
  printList("FILED_TCSCHEDULE_MISMATCH", bad.filedTcMismatch);
  printList("GEOFENCE_FALLBACK_ALLOW", bad.sdspFallback);
  printList("SDSP_ERROR", bad.sdspError);
  printList("GEOFENCE_BLOCKED", bad.geofenceBlocked);

  if (!samples.length) {
    console.log("SLAサンプルがありません");
    return;
  }

  const delays = samples.map((s) => s.delayMs).sort((a, b) => a - b);
  const n = delays.length;

  const quant = (q) => delays[Math.floor(q * (n - 1))];
  const avg = delays.reduce((a, b) => a + b, 0) / n;

  const ms = (x) => `${Math.round(x)} ms (${(x / 1000).toFixed(3)} s)`;

  console.log(
    "--- SLA Stats (FiledIn -> NotifyOut, only FLIGHT_PLAN_FILED) ---"
  );
  console.log("COUNT:", n);
  console.log("MIN :", ms(delays[0]));
  console.log("AVG :", ms(avg));
  console.log("MAX :", ms(delays[n - 1]));
  console.log("P50 :", ms(quant(0.5)));
  console.log("P95 :", ms(quant(0.95)));
  console.log("P99 :", ms(quant(0.99)));

  console.log("--- Samples ---");
  for (const s of samples) {
    console.log(
      `flightPlanId=${s.flightPlanId} traceId=${s.traceId} delay=${ms(
        s.delayMs
      )} peers=${s.notifyPeers.join(",")}`
    );
  }
}
