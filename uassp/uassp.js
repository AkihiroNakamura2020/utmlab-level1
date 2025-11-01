import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import yargs from "yargs";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import axios from 'axios';


const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const argv = yargs(process.argv.slice(2))
  .option("name", { type: "string", default: "UASSP_A" })
  .option("port", { type: "number", default: 4001 })
  .option("fims", { type: "string", default: "http://localhost:3000" })
  .option("analyze", { type: "boolean", default: false })
  .help()
  .parse();

if (argv.analyze) {
  analyze();
  process.exit(0);
}

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = argv.port;
const NAME = argv.name;
const FIMS = argv.fims;
// 先頭の定義の後あたりに追加（任意）
const FIMS_BASE = FIMS;
async function postFims(path, data) {
  // axiosは第2引数がbody、第3引数がconfig
  return axios.post(`${FIMS_BASE}${path}`, data, {
    headers: { "Content-Type": "application/json" }
  });
}


const LOGDIR = path.join(__dirname, "../logs");
fs.mkdirSync(LOGDIR, { recursive: true });
const LOGFILE = path.join(LOGDIR, `${NAME}.ndjson`);

function nowISO() { return new Date().toISOString(); }

//function logUASSP(obj) { fs.appendFileSync(LOGFILE, JSON.stringify(obj) + "\n"); }
function logUASSP(obj) {
     try {
       fs.appendFileSync(LOGFILE, JSON.stringify(obj) + "\n");
       console.log(`[LOG] -> ${LOGFILE} (${obj.msg})`);
     } catch (e) {
       console.error("[LOG WRITE ERROR]", e.message, "path:", LOGFILE);
     }
   }

const ajv = new Ajv({ allErrors: true });
addFormats(ajv); 
const flightplanSchema = JSON.parse(fs.readFileSync(path.join(__dirname, "../common/schema/flightplan.schema.json")));
const eventSchema = JSON.parse(fs.readFileSync(path.join(__dirname, "../common/schema/event.schema.json")));
const validatePlan = ajv.compile(flightplanSchema);
const validateEvent = ajv.compile(eventSchema);

let currentPlan = null;

// Register to FIMS
async function registerToFIMS() {
  try {
    const r = await postFims("/register", {
      name: NAME,
      webhook: `http://localhost:${PORT}/notify`   // ★ /notify を付ける
    });
    if (r.status !== 200 || !r.data?.ok) console.error("FIMS register failed", r.status, r.data);
  } catch (e) {
    console.error("FIMS register error:", e.message);
  }
}


// Submit flight plan (pilot -> UASSP -> FIMS)
app.post("/api/flightplans", async (req, res) => {
  const plan = req.body;
  if (!validatePlan(plan)) return res.status(400).json({ error: "invalid plan", details: validatePlan.errors });
  currentPlan = plan;
  try {

    // FIMS最小実装では /events だけがある想定。イベントとして投げる
   const body = { from: NAME, type: "FLIGHT_PLAN_FILED", payload: plan, ts: Date.now() };
    const r = await postFims("/events", body);
    const ack = r.data ?? { ok: true };


    logUASSP({ ts: nowISO(), who: NAME, msg: "PLAN_SUBMITTED", flightPlanId: plan.flightPlanId });
    res.json(ack);
  } catch (e) {
    res.status(500).json({ error: "FIMS unreachable" });
  }
});

// Receive telemetry (sim -> UASSP) & detect deviation
app.post("/api/telemetry", async (req, res) => {
  const tel = req.body; // { lat, lon, alt, ts }
  res.json({ ok: true });
  if (!currentPlan) return;
  const dev = isDeviated(currentPlan, tel);
  if (dev) {
    const evt = {
      eventId: `evt-${Date.now()}`,
      type: "TACTICAL_DECONFLICT",
      severity: "HIGH",
      subject: { flightPlanId: currentPlan.flightPlanId, uai: currentPlan.aircraft.uai },
      advice: { action: "ALT_CHANGE", targetAlt: Math.min(currentPlan.route.altMax, tel.alt + 10), validForSec: 10 },
      trace: [
        { hop: "UASSP_ORIGIN", name: NAME, t_detect: nowISO() }
      ]
    };
    logUASSP({ ts: nowISO(), who: NAME, msg: "DEVIATION_DETECTED", tel });
    try {
      
      const body = { from: NAME, type: "EVENT", payload: evt, ts: Date.now() };
            await postFims("/events", body);

    } catch (e) {
      console.error("send event error:", e.message);
    }
  }
});

// Receive notify from FIMS (peer side)
app.post("/notify", (req, res) => {
     // FIMSから来るのは {from,type,payload,ts} の封筒
     const envelope = req.body || {};
     const evt = envelope.payload ?? envelope;   // EVENT本体を取り出す
  
     if (!Array.isArray(evt.trace)) evt.trace = [];
     evt.trace.push({ hop: "UASSP_PEER", name: NAME, t_peer_in: nowISO() });
  
     logUASSP({
       ts: nowISO(),
       who: NAME,
       msg: "NOTIFY_IN",
       eventId: evt.eventId,
       trace: evt.trace
     });
     res.json({ ok: true });
   });

// Simple deviation check
function isDeviated(plan, tel) {
  const dist2seg = distancePointToSegment(plan.route.start, plan.route.end, [tel.lon, tel.lat]);
  const horizThreshold = 30; // meters
  const altOk = tel.alt >= plan.route.altMin && tel.alt <= plan.route.altMax;
  return dist2seg > horizThreshold || !altOk;
}

function haversine(a, b) {
  const R = 6371000;
  const toRad = d => d * Math.PI / 180;
  const [lon1, lat1] = a, [lon2, lat2] = b;
  const dLat = toRad(lat2 - lat1), dLon = toRad(lon2 - lon1);
  const h = Math.sin(dLat/2)**2 + Math.cos(toRad(lat1))*Math.cos(toRad(lat2))*Math.sin(dLon/2)**2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

function distancePointToSegment(p1, p2, p) {
  const d1 = haversine(p1, p), d2 = haversine(p2, p), d12 = haversine(p1, p2);
  if (d12 === 0) return d1;
  const proj = Math.max(0, Math.min(1, (d12**2 + d1**2 - d2**2) / (2*d12**2)));
  const closest = [
    p1[0] + (p2[0]-p1[0]) * proj,
    p1[1] + (p2[1]-p1[1]) * proj
  ];
  return haversine(closest, p);
}

async function start() {
  await registerToFIMS();
  app.listen(PORT, () => console.log(`${NAME} listening on http://localhost:${PORT}`));
  logUASSP({ ts: nowISO(), who: NAME, msg: "BOOT" });
}
start();

// Analyzer: compute P50/P95/P99 from logs
function analyze() {
  const dir = path.join(__dirname, "../logs");
  if (!fs.existsSync(dir)) return console.log("no logs dir");
  const files = fs.readdirSync(dir).filter(f => f.endsWith(".ndjson"));
  const samples = [];
  for (const f of files) {
    const lines = fs.readFileSync(path.join(dir, f), "utf-8").trim().split("\n");
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        if (obj.msg === "NOTIFY_IN" && Array.isArray(obj.trace)) {
          const t0 = obj.trace.find(t => t.hop === "UASSP_ORIGIN")?.t_detect;
          const tN = obj.trace.find(t => t.hop === "UASSP_PEER")?.t_peer_in;
          if (t0 && tN) samples.push(new Date(tN) - new Date(t0));
        }
      } catch {}
    }
  }
  if (!samples.length) return console.log("no events");
  samples.sort((a,b)=>a-b);
  const quant = q => samples[Math.floor(q*(samples.length-1))];
  const ms = n => `${n} ms (${(n/1000).toFixed(3)} s)`;
  console.log(`Samples: ${samples.length}`);
  console.log(`P50: ${ms(quant(0.50))}  P95: ${ms(quant(0.95))}  P99: ${ms(quant(0.99))}`);
}
