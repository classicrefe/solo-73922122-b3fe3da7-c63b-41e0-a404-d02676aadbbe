"""端到端冒烟/随机化测试: 通过 HTTP 打真实服务, 用独立重算对照器核对索引。

仅用标准库。用法:
  .venv/bin/python tests/e2e_http.py http://127.0.0.1:8000
"""
from __future__ import annotations

import json
import random
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
STATIONS = ["A", "B", "C", "D"]


def call(method: str, path: str, body: dict | None = None,
         expect: tuple[int, ...] = (200, 201)) -> tuple[int, dict | list | None]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            code = resp.status
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        code = e.code
        payload = json.loads(e.read().decode())
    assert code in expect, f"{method} {path} -> {code} {payload}, 期望 {expect}"
    return code, payload


def oracle(scans: list[dict]) -> dict[tuple, dict]:
    """独立于服务实现的全量重算: 返回 (from,to) -> 工件集合/事件数, 按天。"""
    by_part: dict[str, list[dict]] = defaultdict(list)
    for s in scans:
        by_part[s["part_id"]].append(s)
    # (part, day, from, to) -> 次数
    edges: dict[tuple, int] = defaultdict(int)
    for part, items in by_part.items():
        items.sort(key=lambda x: (x["ts_utc"], x["scan_id"]))
        for a, b in zip(items, items[1:]):
            if a["station"] != b["station"]:
                edges[(b["part_id"], b["day_utc"],
                       a["station"], b["station"])] += 1
    return edges


def main() -> int:
    rng = random.Random(20261124)
    print("0. 健康检查/文档")
    call("GET", "/health")
    with urllib.request.urlopen(BASE + "/openapi.json", timeout=10) as r:
        spec = json.loads(r.read().decode())
    assert "/scans" in spec["paths"] and "/flows" in spec["paths"]

    print("1. 固定场景: 录入 + 日期归属 + 查询")
    call("POST", "/scans", {"scan_id": "e1", "part_id": "E1", "station": "A",
                            "ts_utc": "2026-11-23T23:50:00Z"})
    call("POST", "/scans", {"scan_id": "e2", "part_id": "E1", "station": "B",
                            "ts_utc": "2026-11-24T00:10:00Z"})
    # 重复编号 -> 409 且数据不变
    _, before = call("GET", "/scans?part_id=E1")
    code, _ = call("POST", "/scans",
                   {"scan_id": "e1", "part_id": "E1", "station": "Z",
                    "ts_utc": "2026-11-24T03:00:00Z"}, expect=(409,))
    _, after = call("GET", "/scans?part_id=E1")
    assert before == after, "重复编号失败后数据发生变化"
    # 流向归入后一次扫码的 UTC 日期 11-24
    _, d23 = call("GET", "/flows?start_day=2026-11-23&end_day=2026-11-23")
    _, d24 = call("GET", "/flows?start_day=2026-11-24&end_day=2026-11-24")
    assert d23 == [] and len(d24) == 1 and d24[0]["from_station"] == "A" \
        and d24[0]["to_station"] == "B" and d24[0]["part_count"] == 1

    print("2. 改时刻/改工位 + 404/422")
    call("PATCH", "/scans/e2", {"station": "C"})
    call("PATCH", "/scans/e2", {"ts_utc": "2026-11-24T01:00:00Z"})
    call("PATCH", "/scans/missing", {"station": "X"}, expect=(404,))
    call("PATCH", "/scans/e1", {}, expect=(422,))
    call("POST", "/scans", {"scan_id": "bad", "part_id": "X", "station": "A",
                            "ts_utc": "nonsense"}, expect=(422,))
    call("GET", "/flows?start_day=2026-11-24&end_day=2026-11-01",
         expect=(422,))

    print("3. 撤回 + 不存在 -> 404")
    call("DELETE", "/scans/e1")
    call("DELETE", "/scans/e1", expect=(404,))

    print("4. 随机化增/改/撤, 每步与独立重算对照")
    scans: list[dict] = []
    base = datetime(2026, 11, 20, 8, 0, tzinfo=timezone.utc)
    next_id = 0

    def new_id() -> str:
        nonlocal next_id
        next_id += 1
        return f"r{next_id:04d}"

    # 预填
    for part in ("R1", "R2", "R3"):
        for k in range(4):
            sid = new_id()
            ts = base + timedelta(hours=rng.randint(0, 72), minutes=rng.randint(0, 59))
            body = {"scan_id": sid, "part_id": part,
                    "station": rng.choice(STATIONS),
                    "ts_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ")}
            call("POST", "/scans", body)
            scans.append({"scan_id": sid, "part_id": part,
                          "station": body["station"],
                          "ts_utc": ts.astimezone(timezone.utc).strftime(
                              "%Y-%m-%dT%H:%M:%S.%fZ"),
                          "day_utc": ts.astimezone(timezone.utc).strftime("%Y-%m-%d")})

    def sync_check():
        _, remote_all = call("GET", "/scans")
        # 固定场景的 e2 等数据不属于本轮随机化状态, 只对照 R* 工件
        remote = [r for r in remote_all if r["part_id"].startswith("R")]
        remote_sorted = sorted(remote, key=lambda x: (x["part_id"], x["ts_utc"], x["scan_id"]))
        local_sorted = sorted(scans, key=lambda x: (x["part_id"], x["ts_utc"], x["scan_id"]))
        assert remote_sorted == local_sorted, "扫码明细与本地状态不一致"
        _, idx_all = call("GET", "/flow-index")
        idx = [r for r in idx_all if r["part_id"].startswith("R")]
        expected = oracle(scans)
        actual = {(r["part_id"], r["day_utc"], r["from_station"],
                   r["to_station"]): r["ref_count"] for r in idx}
        assert actual == dict(expected), (
            f"流向索引不一致\n期望={sorted(expected.items())}\n实际={sorted(actual.items())}")
        return expected, actual

    sync_check()
    for step in range(120):
        action = rng.choices(["add", "update_station", "update_time", "withdraw"],
                             weights=[4, 2, 2, 2])[0]
        if action == "add" or not scans:
            sid = new_id()
            part = rng.choice(["R1", "R2", "R3", "R4"])
            ts = base + timedelta(hours=rng.randint(0, 96), minutes=rng.randint(0, 59))
            station = rng.choice(STATIONS)
            call("POST", "/scans", {"scan_id": sid, "part_id": part,
                                    "station": station,
                                    "ts_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ")})
            ts_u = ts.astimezone(timezone.utc)
            scans.append({"scan_id": sid, "part_id": part, "station": station,
                          "ts_utc": ts_u.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                          "day_utc": ts_u.strftime("%Y-%m-%d")})
        else:
            target = rng.choice(scans)
            if action == "withdraw":
                call("DELETE", f"/scans/{target['scan_id']}")
                scans = [s for s in scans if s["scan_id"] != target["scan_id"]]
            elif action == "update_station":
                new_station = rng.choice([s for s in STATIONS
                                          if s != target["station"]])
                call("PATCH", f"/scans/{target['scan_id']}",
                     {"station": new_station})
                target["station"] = new_station
            else:
                ts = base + timedelta(hours=rng.randint(0, 96),
                                      minutes=rng.randint(0, 59))
                call("PATCH", f"/scans/{target['scan_id']}",
                     {"ts_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ")})
                ts_u = ts.astimezone(timezone.utc)
                target["ts_utc"] = ts_u.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                target["day_utc"] = ts_u.strftime("%Y-%m-%d")
        expected, actual = sync_check()

    print(f"   120 轮随机操作后: 扫码 {len(scans)} 条, 索引边 {len(actual)} 条, 全部一致")

    print("5. 区间统计语义: 同件同区间同流向只算一次")
    _, idx = call("GET", "/flow-index")
    # 本地按 (from,to) 聚合: 工件去重计数, 事件数求和
    exp_agg: dict[tuple, set] = defaultdict(set)
    exp_events: dict[tuple, int] = defaultdict(int)
    for (part, day, frm, to), cnt in expected.items():
        exp_agg[(frm, to)].add(part)
        exp_events[(frm, to)] += cnt
    _, flows = call("GET", "/flows?start_day=2026-11-20&end_day=2026-11-30")
    got = {(f["from_station"], f["to_station"]):
           (f["part_count"], f["event_count"]) for f in flows}
    for key, parts in exp_agg.items():
        assert got[key] == (len(parts), exp_events[key]), f"聚合不一致: {key}"
    # 索引行数 = 去重后的 (件,天,流向) 组合数, 与事件次数解耦
    total_events = sum(r["ref_count"] for r in idx)
    assert len(idx) <= total_events
    print(f"   流向 {len(flows)} 种, 索引行 {len(idx)} 行 / 事件 {total_events} 次")

    print("全部端到端检查通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
