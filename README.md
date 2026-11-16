# 公交逐站预计到站 · 数据整理平台

把 GTFS 运行消息整理成站牌可读取的**逐站预计到站数据**。三个独立进程，各自有独立的
SQLite 数据库：

| 服务 | 端口 | 职责 | 持久化 |
|------|------|------|--------|
| `webapp` 采集网页 (FastAPI) | 8000 | 上传计划、持续拉取 GTFS-RT、关联、推送、对账 | `webapp_data` 卷 |
| `gateway` 模拟实时网关 | 8001 | 输出完整 GTFS-RT 快照，可设慢响应/故障 | 无（测试桩，内存态） |
| `sign` 独立持久化站牌服务 | 8002 | 落库完整明细、版本守卫防回退、可模拟丢回执 | `sign_data` 卷 |

## 一、启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 采集网页   http://localhost:8000/
# 站牌实际内容 http://localhost:8002/
# 网关管理文档 http://localhost:8001/docs
```

### 无 Docker 环境（本机 Python）

```bash
python -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
./run_local.sh        # 后台 worker 开启，自动轮询/推送
# 或
./run_smoke.sh        # 后台 worker 关闭，便于脚本确定性驱动
```

## 二、计划 CSV 上传

`POST /api/schedule`（multipart：`file` + 可选 `service_date=YYYY-MM-DD`），网页首页即可上传。

* 列名别名（大小写/空格不敏感）：
  `trip_id|班次`、`stop_sequence|站序`、`stop_code|站号`、`scheduled_time|计划时刻|计划时间`
* 计划时刻（UTC）支持：`HH:MM[:SS]`（按服务日解释）、ISO-8601（`...Z` 或带偏移）、epoch 秒
* 编码支持 UTF-8（含 BOM）与 GB18030
* 重新上传会**原子重置**当日全部实时状态、快照与推送记录

示例见 `examples/schedule_sample.csv`。

## 三、核心语义（与需求逐条对应）

1. **完整快照关联**：每次拉取的是完整 `FeedMessage`，按 `trip_id` + `stop_sequence`
   与当日计划关联；只处理**绝对预计时刻**（`arrival/departure.time`），不使用相对
   `delay`；识别班次级 `CANCELED`（整班取消）与站点级 `SKIPPED`（甩站），`NO_DATA`
   站点不产生预测、显示待定。
2. **缺席即撤销 → 待定**：新完整快照里没有的班次，其实时状态在同一事务中被整体替换，
   LEFT JOIN 无实时行，站牌状态回到 `PENDING`（待定），旧预测/取消不再保留。
3. **拉取/解析失败保留上次成功 + 标过期**：网络错误、超时、HTTP 非 2xx、protobuf
   解析失败、header 缺失都不触碰已应用状态，只把轮询状态置 `ERROR`、网页显示
   `STALE 已过期` 红标。
4. **迟到快照不能回退**：以 `header.timestamp` 为准，非递增（≤ 已应用版本）的快照
   记为 rejected，结果不变；快照表里能看到拒绝原因。
5. **完整明细推送 + 本地/站牌版本区分**：推送载荷是当日全部班次逐站明细的**完整期望
   状态**，带单调 `version`、`idempotency_key` 与规范 JSON `content_hash`。网页同步卡片
   分别显示「本地最新版本」与「站牌实际生效版本」。
6. **丢回执 / 并发 / 重启后可核实接续**：
   * 站牌**先落库后回包**；回执丢失（HTTP500/挂起）时数据已持久。
   * 每次推送前先 `GET /state` **对账**：站牌已在提供同版本同哈希内容时，把本地
     IN_FLIGHT/FAILED 尝试按哈希确认，无需重复推送。
   * 同版本同幂等键重放为幂等 no-op；`claim_next_push` 用事务保证并发只有一个在途。
   * 应用重启把残留 IN_FLIGHT 尝试标记为 FAILED（结果未知），由对账确认或幂等重试接续。
7. **迟到旧推送不能回退站牌**：站牌对 `version < 当前版本` 的 POST 一律 `409` 拒绝。
8. **站牌离线不阻塞采集**：推送与轮询是两个独立循环；推送失败只记 FAILED 并重试，
   轮询照常推进，站牌恢复后自动追到最新版本。

## 四、场景操作手册（curl）

```bash
# 1) 网关注入一个完整快照（header_ts 须随每次更新递增）
curl -X POST localhost:8001/admin/snapshot -H 'Content-Type: application/json' -d '{
  "header_ts": 1700000000,
  "entities": [
    {"trip_id":"T1001","stops":[
      {"seq":1,"arrival":1700000100,"departure":1700000110},
      {"seq":2,"arrival":1700000400,"departure":1700000410}]},
    {"trip_id":"T1002","cancelled":true}
  ]}'

# 2) 慢响应 / 故障模式（网页会进入 STALE，状态保留）
curl -X POST localhost:8001/admin/delay    -H 'Content-Type: application/json' -d '{"delay":8}'
curl -X POST localhost:8001/admin/failure  -H 'Content-Type: application/json' -d '{"mode":"MALFORMED"}'
# mode: NONE | SLOW | DROP | MALFORMED | EMPTY | HTTP500
curl -X POST localhost:8001/admin/failure  -H 'Content-Type: application/json' -d '{"mode":"NONE"}'

# 3) 站牌模拟“落库后丢回执”
curl -X POST localhost:8002/admin/lose-receipt -H 'Content-Type: application/json' -d '{"mode":"HTTP500"}'
# mode: OFF | HANG（提交后响应挂起）| HTTP500（提交后 500）
curl -X POST localhost:8002/admin/lose-receipt -H 'Content-Type: application/json' -d '{"mode":"OFF"}'

# 4) 手动控制（网页按钮等价）
curl -X POST localhost:8000/api/poll-now       # 立即拉取一次
curl -X POST localhost:8000/api/reconcile-now  # 立即对账站牌
curl -X POST localhost:8000/api/push-now       # 立即推送一次
curl        localhost:8000/api/sync            # 同步状态/快照历史/推送尝试
curl        localhost:8000/api/trips           # 逐站明细
curl        localhost:8000/api/evidence/T1001  # 单班逐站依据
```

## 五、目录结构

```
webapp/            采集网页：config / storage(SQLite) / schedule_io / gtfs_rt /
                   payload / sync(轮询+推送) / main(FastAPI) / static(前端)
gateway/app.py     可配置慢响应与故障模式的 GTFS-RT 模拟网关
sign/app.py        独立持久化站牌（版本守卫、幂等、丢回执模拟）+ 查看页
tests/             24 个 pytest（单元 + 三服务进程内交错集成）
scripts/smoke_e2e.py  真实 uvicorn 进程的端到端冒烟（24 项检查）
docker-compose.yml Dockerfile  run_local.sh  run_smoke.sh
```

## 六、自测

```bash
.venv/bin/python -m pytest -q          # 24 passed
./run_smoke.sh &                        # 另起终端：
.venv/bin/python scripts/smoke_e2e.py  # ALL SMOKE CHECKS PASSED
```

覆盖的关键交错：缺席撤销待定、解析/拉取失败保状态标过期、慢网关超时、迟到快照拒绝、
整班取消、旧推送 409 防回退、站牌离线不阻塞采集、丢回执后对账确认、挂起后幂等重试、
重启接续、并发推送单一在途。
