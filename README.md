# utm-lab (Beginner / Local / Mac)

Node.js と Python だけで動く **UTM 検証の最小構成**。  
FIMS（統合ハブ）— UASSP（運航管理）— シミュレータ の流れで、**I/F 整合** と **通知遅延** を確認できます。

## 1) 必要なもの

- Node.js 18+（LTS 推奨）
- Python 3.9+

Mac (Homebrew) 例：
```bash
brew install node python
```

## 2) 依存インストール

```bash
cd utm-lab
npm install
```

## 3) 起動（ターミナルを 3〜4 個）

### ① FIMS
```bash
npm run fims
# FIMS listening on http://localhost:3000
```

### ② UASSP を 2 つ起動
**ターミナル2**
```bash
npm run uasspA
# UASSP_A listening on http://localhost:4001
```
**ターミナル3**
```bash
npm run uasspB
# UASSP_B listening on http://localhost:4002
```

> 起動時に各 UASSP は FIMS に自動登録されます。

### ③ シミュレータ実行（A 側へテレメトリ送信）
**ターミナル4**
```bash
npm run simA
```

- 走行の 60% で故意に逸脱します。
- UASSP_A が検知 → FIMS へイベント → FIMS から UASSP_B へ通知。

### ④ 遅延の集計（P50/P95/P99）
```bash
npm run analyze
```
`logs/` フォルダの NDJSON から、**検知→通知受信** の E2E 遅延を集計します。

## 4) つまずき対処

- **ポート使用中**: `npm run uasspB` の `--port` を別番号に変更。  
- **Python コマンド**: `python3` がない場合は `python`。  
- **時刻同期**: 本検証は OS 時刻で測定。実証では PTP/NTP を追加してください。

## 5) 次の一歩（任意）

- MQTT/EventBus や mTLS の追加版も拡張可能です。
- ジオフェンスを厳密にするなら `turf` の導入を検討してください。
