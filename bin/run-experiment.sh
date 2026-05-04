#!/bin/bash
# 比較実験スクリプト
# 使い方: ./bin/run-experiment.sh [フライト数] [間隔秒]
FLIGHTS=${1:-5}
INTERVAL=${2:-2}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "=== 実験開始: ${FLIGHTS}機 ${INTERVAL}秒間隔 ==="
mkdir -p results logs

# ログをクリア
rm -f logs/*.ndjson

# システム起動
source env/dev.env
./bin/run-dev.sh &
SYS_PID=$!
sleep 5

# 実験実行
python3 simulator/sim_multi.py \
  --flights $FLIGHTS \
  --interval $INTERVAL \
  --out results/experiment_${TIMESTAMP}.csv

sleep 2

# 結果分析
echo "=== FIMS SLA 分析 ==="
node fims/fims.js --analyze

# 後片付け
kill $SYS_PID 2>/dev/null
echo "=== 実験完了: results/experiment_${TIMESTAMP}.csv ==="
