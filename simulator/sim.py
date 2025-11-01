import argparse, time, requests
from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def lerp(a, b, t):
    return (a[0] + (b[0]-a[0])*t, a[1] + (b[1]-a[1])*t)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uassp", required=True, help="UASSP base URL, e.g. http://localhost:4001")
    ap.add_argument("--deviate", type=float, default=0.6, help="0-1 時点でコース逸脱させる")
    ap.add_argument("--interval", type=float, default=0.5, help="送信間隔(秒)")
    args = ap.parse_args()

    # 名駅 -> 栄 の直線プラン（経度,緯度）
    plan = {
        "flightPlanId": "fp-demo-001",
        "operator": {"id":"ops-123","org":"DemoOrg"},
        "aircraft": {"uai":"JPN-REG-001","rid":"RID-XYZ"},
        "route": {
            "start": [136.8816, 35.1709],
            "end":   [136.9066, 35.1709],
            "altMin": 50, "altMax": 120,
            "tStart": "2025-10-24T05:00:00Z",
            "tEnd":   "2025-10-24T05:20:00Z"
        },
        "intent": "LOGISTICS",
        "safety": {"lostLink":"RTH-60m","parachute": True}
    }
    r = requests.post(f"{args.uassp}/api/flightplans", json=plan, timeout=5)
    r.raise_for_status()
    print("Flight plan submitted:", r.json())

    start = plan["route"]["start"]; end = plan["route"]["end"]
    steps = 40
    for i in range(steps+1):
        t = i/steps
        lon, lat = lerp(start, end, t)
        alt = 80
        # 指定割合以降はわざと逸脱（約90m相当）
        if t >= args.deviate:
            lon += 0.001
        tel = {"lon": lon, "lat": lat, "alt": alt, "ts": now_iso()}
        try:
            requests.post(f"{args.uassp}/api/telemetry", json=tel, timeout=2)
        except Exception as e:
            print("telemetry send error:", e)
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
