"""服务层单元测试: 增量流向维护、去重、修改/撤回、事务回滚。

运行: .venv/bin/python -m pytest tests/test_service.py -q
(无 pytest 时可直接执行: .venv/bin/python tests/test_service.py)
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db, services  # noqa: E402


@pytest.fixture()
def conn(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(db, "DB_PATH", db_path)
    monkeypatch.setattr(db, "_conn", None)
    db.init_db()
    c = db.get_conn()
    yield c
    c.close()
    monkeypatch.setattr(db, "_conn", None)


def _oracle(conn: sqlite3.Connection) -> set[tuple]:
    """从 scans 全量重算期望的流向索引 (含期望引用数)。"""
    rows = conn.execute(
        "SELECT part_id, station, ts_utc, scan_id, day_utc FROM scans"
        " ORDER BY part_id, ts_utc, scan_id"
    ).fetchall()
    by_part: dict[str, list] = {}
    for r in rows:
        by_part.setdefault(r["part_id"], []).append(r)
    expected: dict[tuple, int] = {}
    for scans in by_part.values():
        for a, b in zip(scans, scans[1:]):
            if a["station"] == b["station"]:
                continue
            key = (b["part_id"], b["day_utc"], a["station"], b["station"])
            expected[key] = expected.get(key, 0) + 1
    actual = {
        (r["part_id"], r["day_utc"], r["from_station"], r["to_station"]):
            r["ref_count"]
        for r in conn.execute("SELECT * FROM flow_index")
    }
    assert actual == expected
    return set(expected)


def _idx_rows(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM flow_index").fetchone()[0]


def test_basic_flows_and_day_attribution(conn):
    # 同件 P1: A -> B -> B -> C, 中间同工位不产生流向
    services.add_scan("s1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("s2", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("s3", "P1", "B", "2026-11-24T10:00:00Z")
    services.add_scan("s4", "P1", "C", "2026-11-24T11:00:00Z")
    _oracle(conn)
    flows = {(f["from_station"], f["to_station"]): f["part_count"]
             for f in services.query_flows("2026-11-24", "2026-11-24")}
    assert flows == {("A", "B"): 1, ("B", "C"): 1}


def test_flow_attributed_to_later_scan_day(conn):
    # 23:50 在 A, 次日 00:10 到 B -> 流向归入 11-25
    services.add_scan("s1", "P1", "A", "2026-11-24T23:50:00Z")
    services.add_scan("s2", "P1", "B", "2026-11-25T00:10:00Z")
    _oracle(conn)
    assert services.query_flows("2026-11-24", "2026-11-24") == []
    one = services.query_flows("2026-11-25", "2026-11-25")
    assert len(one) == 1 and one[0]["from_station"] == "A" \
        and one[0]["to_station"] == "B"


def test_same_flow_same_day_dedup_index_rows_do_not_grow(conn):
    # P2 当天两次走 A->B (中间经 C 返回 A: B->C, C->A 不影响 A->B 的去重判断)
    # 序列 A->B->C->A->B 产生 A->B 两次, 索引中 A->B 仅 1 行 ref_count=2
    services.add_scan("a1", "P2", "A", "2026-11-24T08:00:00Z")
    services.add_scan("b1", "P2", "B", "2026-11-24T09:00:00Z")
    services.add_scan("c1", "P2", "C", "2026-11-24T10:00:00Z")
    services.add_scan("a2", "P2", "A", "2026-11-24T11:00:00Z")
    services.add_scan("b2", "P2", "B", "2026-11-24T12:00:00Z")
    ab = conn.execute(
        "SELECT * FROM flow_index WHERE from_station='A' AND to_station='B'"
    ).fetchone()
    assert ab["ref_count"] == 2
    res = { (f["from_station"], f["to_station"]): f
            for f in services.query_flows("2026-11-24", "2026-11-24") }
    assert res[("A", "B")]["part_count"] == 1
    assert res[("A", "B")]["event_count"] == 2
    _oracle(conn)

    # 再来一次 A->B (经 C 回去), 该流向的索引行数仍不随次数增长
    services.add_scan("c2", "P2", "C", "2026-11-24T13:00:00Z")
    services.add_scan("a3", "P2", "A", "2026-11-24T14:00:00Z")
    services.add_scan("b3", "P2", "B", "2026-11-24T15:00:00Z")
    ab_rows = conn.execute(
        "SELECT COUNT(*) FROM flow_index WHERE from_station='A' AND to_station='B'"
    ).fetchone()[0]
    assert ab_rows == 1
    assert conn.execute(
        "SELECT ref_count FROM flow_index"
        " WHERE from_station='A' AND to_station='B'"
    ).fetchone()[0] == 3
    assert res[("A", "B")]["part_count"] == 1
    _oracle(conn)


def test_same_flow_on_two_days_counts_twice_in_range(conn):
    # 同一工件 P3 在两天各走一次 A->B:
    # 区间查询同件同流向只算一次 (part_count=1), 事件数仍为 2
    services.add_scan("a1", "P3", "A", "2026-11-24T08:00:00Z")
    services.add_scan("b1", "P3", "B", "2026-11-24T09:00:00Z")
    services.add_scan("a2", "P3", "A", "2026-11-25T08:00:00Z")
    services.add_scan("b2", "P3", "B", "2026-11-25T09:00:00Z")
    _oracle(conn)
    res = { (f["from_station"], f["to_station"]): f
            for f in services.query_flows("2026-11-24", "2026-11-25") }
    assert res[("A", "B")]["part_count"] == 1
    assert res[("A", "B")]["event_count"] == 2
    # 只查第一天时同样计 1 个工件
    day1 = services.query_flows("2026-11-24", "2026-11-24")
    assert day1[0]["part_count"] == 1
    # 两个不同工件同流向 -> 工件数 2
    services.add_scan("a3", "P4", "A", "2026-11-25T10:00:00Z")
    services.add_scan("b3", "P4", "B", "2026-11-25T11:00:00Z")
    _oracle(conn)
    res2 = { (f["from_station"], f["to_station"]): f
             for f in services.query_flows("2026-11-24", "2026-11-25") }
    assert res2[("A", "B")]["part_count"] == 2


def test_insert_in_middle_only_touches_neighbors(conn):
    services.add_scan("s1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("s3", "P1", "C", "2026-11-24T12:00:00Z")
    assert _idx_rows(conn) == 1  # A->C
    # 中间插入 B: A->C 被替换为 A->B, B->C
    services.add_scan("s2", "P1", "B", "2026-11-24T10:00:00Z")
    _oracle(conn)
    assert _idx_rows(conn) == 2


def test_update_time_and_station(conn):
    services.add_scan("s1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("s2", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("s3", "P1", "C", "2026-11-24T10:00:00Z")

    # 改工位: s2 B->X
    services.update_scan("s2", station="X", ts_raw=None)
    _oracle(conn)
    flows = {(f["from_station"], f["to_station"]) for f in
             services.query_flows("2026-11-24", "2026-11-24")}
    assert flows == {("A", "X"), ("X", "C")}

    # 改时刻: s1 移到 s3 之后, 排序变为 s2(X)->s3(C)->s1(A)
    services.update_scan("s1", station=None, ts_raw="2026-11-24T11:00:00Z")
    _oracle(conn)
    ordered = [r["scan_id"] for r in services.list_scans("P1")]
    assert ordered == ["s2", "s3", "s1"]

    # 同时改时刻和工位
    services.update_scan("s1", station="D", ts_raw="2026-11-24T07:00:00Z")
    _oracle(conn)
    first = services.list_scans("P1")[0]
    assert first["scan_id"] == "s1" and first["station"] == "D"


def test_withdraw_repairs_edges(conn):
    services.add_scan("s1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("s2", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("s3", "P1", "C", "2026-11-24T10:00:00Z")
    services.withdraw_scan("s2")
    _oracle(conn)
    flows = {(f["from_station"], f["to_station"]) for f in
             services.query_flows("2026-11-24", "2026-11-24")}
    assert flows == {("A", "C")}

    # 撤完后再加回, 索引仍与全量重算一致
    services.add_scan("s2", "P1", "B", "2026-11-24T09:00:00Z")
    _oracle(conn)

    with pytest.raises(services.NotFoundError):
        services.withdraw_scan("missing")


def test_duplicate_scan_id_conflict_and_rollback(conn):
    services.add_scan("s1", "P1", "A", "2026-11-24T08:00:00Z")
    with pytest.raises(services.ConflictError):
        services.add_scan("s1", "P1", "B", "2026-11-24T09:00:00Z")
    # 失败保留原状: 仍只有 1 条扫码, 无流向
    assert conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0] == 1
    assert _idx_rows(conn) == 0


def test_failure_rolls_back_scan_and_index(conn, monkeypatch):
    services.add_scan("s1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("s3", "P1", "C", "2026-11-24T12:00:00Z")
    scans_before = conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
    idx_before = _idx_rows(conn)

    real_add_edge = services._add_edge
    calls = {"n": 0}

    def flaky_add_edge(c, a, b):
        calls["n"] += 1
        if calls["n"] == 2:  # 第二条新边写入时失败
            raise RuntimeError("simulated index failure")
        return real_add_edge(c, a, b)

    monkeypatch.setattr(services, "_add_edge", flaky_add_edge)
    with pytest.raises(RuntimeError):
        services.add_scan("s2", "P1", "B", "2026-11-24T10:00:00Z")
    monkeypatch.setattr(services, "_add_edge", real_add_edge)

    # 扫码与索引一同回滚, 保留原状
    assert conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0] == scans_before
    assert _idx_rows(conn) == idx_before
    _oracle(conn)  # 回滚后索引仍与全量重算一致


def test_timezone_normalization_and_same_timestamp_tiebreak(conn):
    # +08:00 的 16:00 等于 UTC 08:00
    services.add_scan("s1", "P1", "A", "2026-11-24T16:00:00+08:00")
    out = services.list_scans("P1")[0]
    assert out["ts_utc"] == "2026-11-24T08:00:00.000000Z"
    assert out["day_utc"] == "2026-11-24"

    # 同一时刻按 scan_id 排序定前后
    services.add_scan("b2", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("b1", "P1", "C", "2026-11-24T09:00:00Z")
    _oracle(conn)
    ordered = [r["scan_id"] for r in services.list_scans("P1")]
    assert ordered == ["s1", "b1", "b2"]


def test_multiple_parts_are_independent(conn):
    services.add_scan("x1", "P1", "A", "2026-11-24T08:00:00Z")
    services.add_scan("x2", "P1", "B", "2026-11-24T09:00:00Z")
    services.add_scan("y1", "P2", "A", "2026-11-24T08:30:00Z")
    services.add_scan("y2", "P2", "C", "2026-11-24T09:30:00Z")
    _oracle(conn)
    flows = {(f["from_station"], f["to_station"]): f["part_count"]
             for f in services.query_flows("2026-11-24", "2026-11-24")}
    assert flows == {("A", "B"): 1, ("A", "C"): 1}


def test_invalid_inputs(conn):
    with pytest.raises(ValueError):
        services.add_scan("s9", "P1", "A", "not-a-timestamp")
    with pytest.raises(ValueError):
        services.query_flows("2026-11-24", "2026-13-99")
    with pytest.raises(ValueError):
        services.query_flows("2026-11-25", "2026-11-24")
    with pytest.raises(services.NotFoundError):
        services.update_scan("nope", station="X", ts_raw=None)


if __name__ == "__main__":
    # 无 pytest 环境下的简易回退执行器
    sys.exit(pytest.main([__file__, "-q"]))
