# 返工补登扫码流向核对服务

FastAPI + SQLite。质检员补登漏扫后，系统按修正后的扫码顺序自动维护工位间
**流向索引**，支持按 UTC 日期区间核对每个流向经手的工件数。

## 规则

1. 每次扫码含：唯一编号 `scan_id`、工件号 `part_id`、工位 `station`、UTC 时刻 `ts_utc`。
2. 同一工件的扫码按 **(UTC 时刻, 扫码编号)** 排序；相邻两次扫码工位不同，
   才产生一条 `前站 -> 后站` 的流向，归入**后一次扫码的 UTC 日期**。
3. 同工位相邻不产生流向。
4. 支持改时刻、改工位、撤回；更新时**只定位改动位置前后的相邻扫码**做增量修复，
   不做全表重算。
5. 扫码明细与流向索引在**同一事务**内提交，任一步失败整体回滚，保留原状。
6. 流向索引按 `(工件, 日期, 前站, 后站)` 主键去重，重复出现只累加 `ref_count`：
   - 同件同日重复走同一流向，索引行数不随次数增长；
   - 区间查询只读索引，同件在区间内走同一流向只算一个工件
     （`COUNT(DISTINCT part_id)`），另给 `event_count` 反映实际发生次数。

## 启动

### Docker Compose（交付方式）

```bash
docker compose up --build
# 服务: http://localhost:8000  交互文档: http://localhost:8000/docs
```

SQLite 数据存于命名卷 `scanflow-data`（容器内 `/data/scanflow.db`）。

### 本机直接运行（无 Docker 时）

```bash
python3 -m venv --without-pip .venv        # 若 ensurepip 缺失, 需先引导 pip
.venv/bin/pip install -r requirements.txt
SCANFLOW_DB_PATH=./data/scanflow.db \
  .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## API（/docs 可直接录入和查数）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/scans` | 录入扫码（编号重复返回 409） |
| PATCH | `/scans/{scan_id}` | 改时刻 / 改工位（只传要改的字段） |
| DELETE | `/scans/{scan_id}` | 撤回扫码并修复相邻流向 |
| GET | `/scans?part_id=` | 查询扫码明细（按件/时刻/编号排序） |
| GET | `/flows?start_day=&end_day=` | **按日期区间统计各流向工件数（只读索引）** |
| GET | `/flow-index?part_id=` | 查看索引明细与引用计数（核对用） |
| GET | `/health` | 健康检查 |

时刻接受 ISO8601（`...Z` 或带偏移量如 `+08:00`），内部统一转 UTC 存储；
日期参数格式为 `YYYY-MM-DD`。

### 示例

```bash
curl -X POST localhost:8000/scans -H 'Content-Type: application/json' \
  -d '{"scan_id":"S1","part_id":"P1","station":"A","ts_utc":"2026-11-24T08:00:00Z"}'
curl -X PATCH localhost:8000/scans/S1 -H 'Content-Type: application/json' \
  -d '{"station":"B"}'
curl -X DELETE localhost:8000/scans/S1
curl "localhost:8000/flows?start_day=2026-11-24&end_day=2026-11-25"
```

`/flows` 返回：

```json
[
  {"from_station": "A", "to_station": "B", "part_count": 3, "event_count": 5}
]
```

## 存储结构

- `scans`：扫码明细，主键 `scan_id`，索引 `(part_id, ts_utc, scan_id)` 支撑相邻定位。
- `flow_index`：流向索引，主键 `(part_id, day_utc, from_station, to_station)`，
  `ref_count` 记录该件当天该流向的出现次数。

## 测试

```bash
.venv/bin/python -m pytest tests/test_service.py -q       # 服务层: 增量/去重/回滚
.venv/bin/python -I tests/e2e_http.py http://127.0.0.1:8000  # HTTP 端到端 + 随机化对照
```

端到端脚本内置独立的全量重算"对照器"，在 120 轮随机增/改/撤后逐行核对索引，
并验证区间统计的去重语义。
