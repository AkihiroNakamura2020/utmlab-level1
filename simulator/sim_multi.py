"""
sim_multi.py - UASSP_A / UASSP_B に同時に複数フライトを申請する比較実験スクリプト

使い方:
  python3 simulator/sim_multi.py \
    --flights 10 \
    --interval 1.0 \
    --uassp-a http://localhost:4001 \
    --uassp-b http://localhost:4002 \
    --out results/experiment_$(date +%Y%m%d_%H%M%S).csv
"""

import argparse
import csv
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

# tc-map.js の5ノード（lon, lat）
NODES = {
    "A": (136.9,    35.18),
    "B": (136.8991, 35.18),
    "C": (136.9009, 35.18),
    "D": (136.9,    35.1809),
    "E": (136.9,    35.1791),
}

# 有向ルートのペア（from, to）
ROUTE_PAIRS = [
    ("A", "B"),
    ("A", "C"),
    ("A", "D"),
    ("A", "E"),
    ("B", "A"),
    ("B", "C"),
    ("C", "A"),
    ("C", "B"),
    ("D", "A"),
    ("E", "A"),
]

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def make_plan(route_pair, flight_idx):
    """
    ルートペアからフライトプランを生成する。
    flightPlanId は UASSP が発行するので含めない。
    """
    from_id, to_id = route_pair
    start = list(NODES[from_id])
    end   = list(NODES[to_id])

    return {
        "operator": {"id": "ops-multi", "org": "MultiExpOrg"},
        "aircraft": {"uai": f"JPN-MULTI-{flight_idx:03d}", "rid": f"RID-M{flight_idx:03d}"},
        "route": {
            "start": start,
            "end":   end,
            "altMin": 50,
            "altMax": 120,
            "tStart": now_iso(),
            "tEnd":   now_iso(),
        },
        "intent": "LOGISTICS",
        "safety": {"lostLink": "RTH-60m", "parachute": True},
    }


def submit_and_complete(uassp_url, plan, flight_idx, label):
    """
    フライトを申請し、完了通知まで送る。
    戻り値: dict with flightPlanId, traceId, delayMs, label, uassp, ok, error
    """
    result = {
        "label":        label,
        "flight_idx":   flight_idx,
        "uassp":        uassp_url,
        "flightPlanId": None,
        "traceId":      None,
        "submitMs":     None,
        "assignedMs":   None,
        "delayMs":      None,
        "ok":           False,
        "error":        None,
    }

    try:
        t0 = int(time.time() * 1000)
        r = requests.post(f"{uassp_url}/api/flightplans", json=plan, timeout=10)
        r.raise_for_status()
        resp = r.json()

        fp_id    = resp.get("flightPlanId")
        trace_id = resp.get("traceId")
        t1 = int(time.time() * 1000)

        if not fp_id:
            result["error"] = "missing flightPlanId in response"
            return result

        result["flightPlanId"] = fp_id
        result["traceId"]      = trace_id
        result["submitMs"]     = t0
        result["assignedMs"]   = t1
        result["delayMs"]      = t1 - t0

        # 完了通知
        try:
            cr = requests.post(
                f"{uassp_url}/api/completed",
                json={"flightPlanId": fp_id},
                timeout=5,
            )
            cr.raise_for_status()
        except Exception as e:
            result["error"] = f"completed failed: {e}"
            result["ok"] = True  # 申請は成功しているので ok=True
            return result

        result["ok"] = True
        return result

    except Exception as e:
        result["error"] = str(e)
        return result


def run_experiment(flights, interval, uassp_a, uassp_b, out_path):
    print(f"=== 実験開始: {flights}機 {interval}秒間隔 ===")
    print(f"  UASSP_A: {uassp_a}")
    print(f"  UASSP_B: {uassp_b}")
    print(f"  出力: {out_path}")

    os.makedirs(os.path.dirname(out_path) if os.path.dirname(out_path) else ".", exist_ok=True)

    tasks = []
    for i in range(flights):
        route_pair = ROUTE_PAIRS[i % len(ROUTE_PAIRS)]
        plan_a = make_plan(route_pair, i * 2)
        plan_b = make_plan(route_pair, i * 2 + 1)
        tasks.append((uassp_a, plan_a, i * 2,     f"A-{i}"))
        tasks.append((uassp_b, plan_b, i * 2 + 1, f"B-{i}"))

    results = []
    lock = threading.Lock()

    def worker(uassp_url, plan, flight_idx, label, seq):
        # interval に従ってずらして送信
        time.sleep(seq * interval)
        r = submit_and_complete(uassp_url, plan, flight_idx, label)
        with lock:
            results.append(r)
            status = "OK" if r["ok"] else f"ERR:{r['error']}"
            print(f"  [{label}] fp={r['flightPlanId']} delay={r['delayMs']}ms {status}")
        return r

    with ThreadPoolExecutor(max_workers=min(len(tasks), 20)) as ex:
        futures = [
            ex.submit(worker, uassp, plan, idx, lbl, seq)
            for seq, (uassp, plan, idx, lbl) in enumerate(tasks)
        ]
        for f in as_completed(futures):
            pass  # 結果は worker 内で results に追記済み

    # CSV 出力
    fieldnames = ["label", "flight_idx", "uassp", "flightPlanId", "traceId",
                  "submitMs", "assignedMs", "delayMs", "ok", "error"]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted(results, key=lambda x: x["flight_idx"]):
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    ok_count  = sum(1 for r in results if r["ok"])
    err_count = len(results) - ok_count
    delays    = [r["delayMs"] for r in results if r["delayMs"] is not None]

    print(f"\n=== 実験結果 ===")
    print(f"  総フライト数 : {len(results)}")
    print(f"  成功          : {ok_count}")
    print(f"  失敗          : {err_count}")
    if delays:
        delays.sort()
        n = len(delays)
        avg = sum(delays) / n
        p50 = delays[int(n * 0.50)]
        p95 = delays[int(n * 0.95)]
        print(f"  遅延 AVG      : {avg:.0f} ms")
        print(f"  遅延 P50      : {p50} ms")
        print(f"  遅延 P95      : {p95} ms")
    print(f"  CSV           : {out_path}")


def main():
    ap = argparse.ArgumentParser(description="UTM 比較実験スクリプト")
    ap.add_argument("--flights",   type=int,   default=5,
                    help="フライト数（UASSP_A + B それぞれに申請）")
    ap.add_argument("--interval",  type=float, default=2.0,
                    help="フライト間隔(秒)")
    ap.add_argument("--uassp-a",   default="http://localhost:4001",
                    help="UASSP_A の URL")
    ap.add_argument("--uassp-b",   default="http://localhost:4002",
                    help="UASSP_B の URL")
    ap.add_argument("--out",       default="results/experiment.csv",
                    help="CSV 出力先パス")
    args = ap.parse_args()

    run_experiment(
        flights=args.flights,
        interval=args.interval,
        uassp_a=args.uassp_a,
        uassp_b=args.uassp_b,
        out_path=args.out,
    )


if __name__ == "__main__":
    main()
