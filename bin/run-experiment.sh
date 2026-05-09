#!/bin/bash
# 比較実験スクリプト
#
# 使い方:
#   ./bin/run-experiment.sh [--flights N] [--interval 秒] [--wait 秒]
#
# 引数:
#   --flights N    各UAASSPへの申請便数（デフォルト: 10）
#                  総便数 = N × 2（UASSP_A × N + UASSP_B × N）
#   --interval 秒  便と便の送信間隔（デフォルト: 1.0秒）
#   --wait 秒      申請後 /api/completed を呼ぶまでの待機時間（デフォルト: 8秒）
#                  FIMS の ASSIGNED→FILED サイクル完了を待つ。短すぎると
#                  COMPLETED が FILED より先着し ACTIVE_ADD が失敗する。
#
# 注意:
#   環境変数は .claude/settings.json の env セクションから自動注入される。
#   スタンドアロン実行（Claude Code 外）の場合は以下を先に実行すること:
#     export $(grep -v '^#' env/dev.env | xargs -d '\n')
#
# 例:
#   ./bin/run-experiment.sh                    # 10便×2=20便、間隔1秒
#   ./bin/run-experiment.sh --flights 5        # 5便×2=10便
#   ./bin/run-experiment.sh --flights 10 --interval 0.5 --wait 10

# ── 引数パース ──
FLIGHTS=10
INTERVAL=1.0
WAIT=8

while [[ $# -gt 0 ]]; do
  case $1 in
    --flights)  FLIGHTS="$2";  shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --wait)     WAIT="$2";     shift 2 ;;
    *)          echo "Unknown option: $1"; exit 1 ;;
  esac
done

TOTAL=$((FLIGHTS * 2))

echo "=== 実験開始 ==="
echo "  便数: ${TOTAL}便（UASSP_A×${FLIGHTS} + UASSP_B×${FLIGHTS}）"
echo "  送信間隔: ${INTERVAL}秒 / 完了待機: ${WAIT}秒"
echo "  出力ファイル: sim_multi.py が自動でタイムスタンプ付きCSVを生成"

# ── ディレクトリ準備 ──
mkdir -p results logs

# ── 前プロセス停止・ログクリア ──
pkill -f "fims/fims.js"   2>/dev/null
pkill -f "uassp/uassp.js" 2>/dev/null
pkill -f "ui/server.js"   2>/dev/null
pkill -f "tc/tc-api.js"   2>/dev/null
pkill -f "sdsp/sdsp.js"   2>/dev/null
sleep 1
rm -f logs/*.ndjson logs/*.log

# ── コンポーネント起動（順番を守る: SDSP→TC→FIMS→UASSP→UI）──
node sdsp/sdsp.js  > logs/sdsp.log  2>&1 &
node tc/tc-api.js  > logs/tc.log    2>&1 &
sleep 1
node fims/fims.js  > logs/fims.log  2>&1 &
sleep 1
node uassp/uassp.js --name UASSP_A --port 4001 > logs/uassp_a.log 2>&1 &
node uassp/uassp.js --name UASSP_B --port 4002 > logs/uassp_b.log 2>&1 &
node ui/server.js  > logs/ui.log    2>&1 &
sleep 3

echo ""
echo "--- システム起動完了 ---"
echo "  Dashboard: http://localhost:8080"

# ── 実験実行 ──
# --out 省略: sim_multi.py が Python 側でタイムスタンプ付きパスを自動生成
# --notify-ui: UIサーバーに expectedTotal を通知し capture率を計測可能にする
python3 simulator/sim_multi.py \
  --flights  "$FLIGHTS"  \
  --interval "$INTERVAL" \
  --wait     "$WAIT"     \
  --notify-ui http://localhost:8080

sleep 2

# ── FIMS SLA 分析 ──
echo ""
echo "=== FIMS SLA 分析 ==="
node fims/fims.js --analyze

# ── 後片付け ──
pkill -f "fims/fims.js"   2>/dev/null
pkill -f "uassp/uassp.js" 2>/dev/null
pkill -f "ui/server.js"   2>/dev/null
pkill -f "tc/tc-api.js"   2>/dev/null
pkill -f "sdsp/sdsp.js"   2>/dev/null

echo ""
echo "=== 実験完了 ==="
