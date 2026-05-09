# CLAUDE.md - UTM-LevelL AI 指示書

## 最初にやること（必須）

以下の順番でファイルを全部読んでから実装を開始すること。
読まずに実装しないこと。

### ステップ 1：プロジェクト構造の把握

```
tree -L 3 -I "node_modules|logs"
```

### ステップ 2：全ファイルを読む

以下を順番に読む：

**設計ドキュメント**

- docs/decisions.md
- docs/protocol.md
- docs/RUN_DEV_GUIDE.md
- README.md

**コアロジック**

- fims/fims.js
- fims/semantic-validate.js
- uassp/uassp.js
- sdsp/sdsp.js
- tc/tc-api.js
- tc/tc-map.js
- tc/tc-router.js
- tc/tc-scheduler.js

**設定・スキーマ**

- env/dev.env
- common/schema/flightplan.schema.json
- common/schema/event.schema.json
- package.json

**シミュレータ**

- simulator/sim.py

**起動スクリプト**

- bin/run-dev.sh

### ステップ 3：齟齬の確認

タスクを実行する前に、指示内容と既存コードの間に齟齬がないか確認すること。
齟齬がある場合は実装前に報告し、既存コードに合わせて解決してから進むこと。
ドキュメント類は以前の修正内容に合わせて現段階の内容を修正し、最新の内容を保つこと。

---

## 作業ルール

- 作業対象フォルダは `utm-levelLAI` のみ
- このフォルダ内のファイルは自由に読み書き・作成・削除してよい
- 既存のコードスタイル・ログキー名・env 変数の命名規則を維持する
- 各関数に JSDoc コメントを追加する（既存スタイルに合わせる）
- エラー時は必ず `logFIMS` / `logUASSP` でログを出す
- `activePlans` の操作は必ず後ろからループして splice する
- 実装後は `node --check <ファイル名>` で構文チェックを実行する
- 実装後はコードの実行を行い、結果を再検証し、課題点があった再度コードを作り直す。課題点がなくなるまで続ける。

---

## TC スロット探索の調整

### MAX_DEPART_DELAY_SEC

TC がスロットを探す最大待機時間（秒）。

| 設定箇所 | 値 |
|---|---|
| `.claude/settings.json` の `env` セクション | `"MAX_DEPART_DELAY_SEC": "10"` |
| `env/dev.env` | `MAX_DEPART_DELAY_SEC=10` |

**動作：** 希望出発時刻から N 秒以内に空きスロットが見つからなければ `NO_SLOT_IN_WINDOW` で拒否。

```bash
# 例：窓を30秒に広げる（混雑耐性を上げる）
# .claude/settings.json の env セクションを変更してTCを再起動
"MAX_DEPART_DELAY_SEC": "30"
```

**注意：**
- 大きくすると拒否が減る代わりに出発遅延が増える
- `NO_SLOT_IN_WINDOW` は正常な TC 動作。高密度実験では発生を許容する
- 迂回ルートの探索は現時点で未実装（STEP13c で対応予定）

---

## 既知の設計上の注意事項

### traceId の設計

- **生成**：UAASSP が `crypto.randomUUID()` で生成（申請者が主導）
- **維持**：FIMS が `ensureTraceId` でキャッシュし一気通貫で伝播
- FIMS は「最終決定」ではなく「UAASSP が生成した traceId を維持・保護」する

### TRACE_MODE の実態

- `quarantine` は現状 `strict` と同じ動作（null を返して DROP）
- 「隔離して処理継続」は未実装（名前だけ残っている）

### checkGeofenceWithSdsp の戻り値

- `checked: false` = 「未判定」であり「安全だった」ではない
- `noFlyHits: null` = 未確認（`[]` のヒットなしとは別物）
- `fallback: true` = SDSP 障害によるフォールバック許可

### activePlans の注意

- 後ろからループして splice すること（前からだとインデックスがズレる）
- `expireAtMs` = `filedTc.arrivalAbsMs`（TC の到着時刻が期限）

### 未対処の既知問題（今回は触らない）

1. `peers` Map に削除処理がない
2. JWT_SECRET / HMAC_SECRET が全 UASSP 共有
3. `validateEvent`（uassp.js）が compile されているが呼ばれていない
4. `operator / aircraft / intent / safety` が FILED payload に含まれるが FIMS で未使用
5. `route.tEnd` が schema required だが FIMS・TC ともに未参照

---

## 確認方法

各タスク実装後に実行すること：

```bash
# 構文チェック
node --check fims/fims.js
node --check uassp/uassp.js
node --check ui/server.js  # タスク4の場合

# 起動テスト
set -a && source env/dev.env && set +a
node fims/fims.js &
sleep 1
echo "起動確認OK"
kill %1
```

---

## Claude Code 操作ルール

### Bash 自動承認設定

`.claude/settings.json` に全 Bash コマンドの allowlist が設定済み。
**"Do you want to proceed?" は一切表示しない。** 表示された場合は設定を確認すること。

#### Python heredoc 内で `{"key": value}` を使わない

heredoc 内の `{` と `"` の組み合わせが「展開難読化」と判定されて止まる。
**dict リテラルの代わりに `dict()` コンストラクタか直接 `print()` を使う。**

```python
# ❌ 止まる
rejected.append({"ts": obj.get("ts"), "reason": obj.get("reason")})

# ✅ 止まらない（dict() を使う）
rejected.append(dict(ts=obj.get("ts"), reason=obj.get("reason")))

# ✅ 止まらない（直接 print）
print(obj.get("ts"), obj.get("reason"))
```

#### `$()` コマンド置換を Bash 引数に埋め込まない

`--out results/experiment_$(date +%Y%m%d_%H%M%S).csv` のように `$()` を引数に
埋め込むと、Claude Code が `date` を別コマンドとして権限チェックし止まる場合がある。

**対策：タイムスタンプは Python 側で生成する。**

```python
# sim_multi.py: --out を省略すると Python が自動でタイムスタンプ付きパスを生成
out_path = args.out or f"results/experiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
```

Bash コマンドは `$()` なしのシンプルな形にする：

```bash
# ✅ $() なし
python3 simulator/sim_multi.py --interval 1.0 --wait 8 --notify-ui http://localhost:8080

# ❌ $() 埋め込みは避ける
python3 simulator/sim_multi.py --out results/experiment_$(date +%Y%m%d_%H%M%S).csv
```

#### `source` は使わない

Claude Code のバージョンによっては `source` を含むコマンドが権限プロンプトを
引き起こす場合があるため、このプロジェクトでは `source` を使わない。
代わりに以下を使うこと：

```bash
export $(grep -v '^#' env/dev.env | xargs -d '\n')
```

### 確認なしで実行してよい操作

現在の実験段階では全て確認を取らず実行してよい

- `logs/` 配下のログ削除（`rm -f logs/*.ndjson logs/*.log`）
- `results/` 配下への CSV 出力
- `node --check` / `python3 -m py_compile` による構文確認
- FIMS / UASSP / TC / SDSP / UI の起動・停止
- `pkill -f` による本プロジェクト関連プロセスの停止
- `simulator/sim_multi.py` による実験実行
- `grep` / `jq` / `python3` によるログ分析

### 外部環境接続が始まったら、必ずユーザー確認を取る操作

現在のローカル環境での確認不要。
外部接続を行う場合には下記の手順に従う。

- `rm -rf` による広範囲削除
- `git reset` / `git clean` / `git checkout` による破壊的変更
- `git push`
- `npm install` / `brew install` など環境変更
- `env/dev.env` の秘密値変更
- `package.json` の依存関係追加
- 外部 API キーや認証情報を扱う操作

### env 読み込みの統一形式

`.claude/settings.json` の `env` セクションはバックグラウンドプロセス（`node ... &`）への
注入が保証されないため、スクリプト内で明示的に読み込む。

`bin/run-experiment.sh` では以下の方式を使用（インラインコメントも安全に除去できる）：

```bash
while IFS='=' read -r key val; do
  [[ -z "$key" || "$key" == \#* ]] && continue
  val="${val%%#*}"
  val="${val%"${val##*[! ]}"}"
  export "$key=$val"
done < env/dev.env
```

Claude Code の Bash ツールから直接コマンドを叩く場合（バックグラウンドなし）は
`settings.json` の `env` が注入される。

### 実験実行の標準手順

```bash
# 1. 前プロセス停止
pkill -f "fims/fims.js" 2>/dev/null
pkill -f "uassp/uassp.js" 2>/dev/null
pkill -f "ui/server.js" 2>/dev/null
pkill -f "tc/tc-api.js" 2>/dev/null
pkill -f "sdsp/sdsp.js" 2>/dev/null

# 2. ログクリア
rm -f logs/fims.ndjson logs/fims.log logs/uassp_a.log logs/uassp_b.log logs/ui.log logs/tc.ndjson logs/sdsp.log

# 3. コンポーネント起動（順番を守る）
# 環境変数は .claude/settings.json の env セクションから自動注入されるため読み込み不要
node sdsp/sdsp.js  > logs/sdsp.log  2>&1 &
node tc/tc-api.js  > logs/tc.log    2>&1 &
sleep 1
node fims/fims.js  > logs/fims.log  2>&1 &
sleep 1
node uassp/uassp.js --name UASSP_A --port 4001 > logs/uassp_a.log 2>&1 &
node uassp/uassp.js --name UASSP_B --port 4002 > logs/uassp_b.log 2>&1 &
node ui/server.js  > logs/ui.log    2>&1 &
sleep 2

# 5. 実験実行（--notify-ui でUIに expectedTotal を通知）
python3 simulator/sim_multi.py \
  --flights 10 --interval 1.0 --wait 8 \
  --notify-ui http://localhost:8080 \
  --out results/experiment_$(date +%Y%m%d_%H%M%S).csv
```

### 実験便数の考え方

`sim_multi.py --flights N` は UASSP_A と UASSP_B の両方に N 便ずつ送る。

```
expectedTotal = N × 2
```

UI ダッシュボードの Header には以下を表示する：

```
captured X / expected Y   capture率%   miss:Z
```

### SLA の見方

| 優先度 | 確認項目                          |
| ------ | --------------------------------- |
| 最重要 | captured / expected が 100% か    |
| 重要   | P95 が悪化していないか            |
| 重要   | reject 理由は何か                 |
| 参考   | どのフェーズが遅いか              |
| 参考   | conflict はどの edge で発生したか |

### captured は 100%が原則

飛行申請の補足ミスは大きな事故につながる。
ログだけではなく、帳票を３秒単位で確認するなど必ず
captured は 100%にすること。申請内容を正しく表示されないと UI の意味がなくなってしまうため。

### claude.md の断続的アップデートの提案

修正指示があった際には、今後同様の修正指示がないように自らルールを作成し、claude.md の最終部分に追加して良いかの
確認を行う。その際には追加ルール案も見える様にして確認すること。
