"""核心业务逻辑: 扫码增/改/撤 + 流向索引的增量维护。

规则
----
同件扫码按 (ts_utc, scan_id) 排序。相邻两次扫码工位不同, 才记录一条
前站 -> 后站的流向, 归入"后一次扫码"的 UTC 日期 (day_utc)。

更新时只查"改动位置"前后的相邻扫码:
- 新增: 取该件在 (ts, scan_id) 顺序上的前驱 prev / 后继 next,
        删除旧边 prev->next, 加入 prev->new 与 new->next;
- 撤回: 取 prev / next, 删除 prev->old 与 old->next, 加入 prev->next;
- 修改: 按旧位置先撤一次, 再按新位置插一次。

flow_index 主键去重: 同件同日同流向只占一行, ref_count 记出现次数。
查询按行计数即可, 重复次数增加不会增加索引行数。

每个公开写操作在一个事务内完成, 任何一步失败整体回滚, 保留原状。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from .db import get_conn, write_lock

TS_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"
DAY_FMT = "%Y-%m-%d"


class ConflictError(Exception):
    """scan_id 已存在。"""


class NotFoundError(Exception):
    """scan_id 不存在。"""


def parse_ts(raw: str) -> datetime:
    """解析 ISO8601 时刻并归一化为 UTC。

    接受 'Z' 结尾或带偏移量的 ISO 字符串; 无时区信息时按 UTC 处理。
    """
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"无法解析时刻: {raw!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_ts(dt: datetime) -> str:
    return dt.strftime(TS_FMT)


def day_of(dt: datetime) -> str:
    return dt.strftime(DAY_FMT)


# ---------------------------------------------------------------- 相邻扫码

def _get_scan(conn: sqlite3.Connection, scan_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM scans WHERE scan_id = ?", (scan_id,)
    ).fetchone()


def _prev_row(
    conn: sqlite3.Connection, part_id: str, ts_utc: str, scan_id: str
) -> sqlite3.Row | None:
    """同件中排序严格在 (ts_utc, scan_id) 之前的最近一条。"""
    return conn.execute(
        """
        SELECT * FROM scans
        WHERE part_id = ?
          AND (ts_utc < ? OR (ts_utc = ? AND scan_id < ?))
        ORDER BY ts_utc DESC, scan_id DESC
        LIMIT 1
        """,
        (part_id, ts_utc, ts_utc, scan_id),
    ).fetchone()


def _next_row(
    conn: sqlite3.Connection, part_id: str, ts_utc: str, scan_id: str
) -> sqlite3.Row | None:
    """同件中排序严格在 (ts_utc, scan_id) 之后的最近一条。"""
    return conn.execute(
        """
        SELECT * FROM scans
        WHERE part_id = ?
          AND (ts_utc > ? OR (ts_utc = ? AND scan_id > ?))
        ORDER BY ts_utc ASC, scan_id ASC
        LIMIT 1
        """,
        (part_id, ts_utc, ts_utc, scan_id),
    ).fetchone()


# ---------------------------------------------------------------- 流向边维护

def _add_edge(conn: sqlite3.Connection, a: sqlite3.Row | None,
              b: sqlite3.Row | None) -> None:
    """加入一条 a -> b 的流向边 (b 为后一次扫码, 日期取 b.day_utc)。

    相邻两次工位相同则不产生流向; 去重由主键保证, 重复出现只累加引用数。
    """
    if a is None or b is None:
        return
    if a["station"] == b["station"]:
        return
    conn.execute(
        """
        INSERT INTO flow_index (part_id, day_utc, from_station, to_station, ref_count)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(part_id, day_utc, from_station, to_station)
        DO UPDATE SET ref_count = ref_count + 1
        """,
        (b["part_id"], b["day_utc"], a["station"], b["station"]),
    )


def _remove_edge(conn: sqlite3.Connection, a: sqlite3.Row | None,
                 b: sqlite3.Row | None) -> None:
    """移除一条 a -> b 流向边的一次引用; 引用归零即删除索引行。"""
    if a is None or b is None:
        return
    if a["station"] == b["station"]:
        return
    cur = conn.execute(
        """
        UPDATE flow_index
        SET ref_count = ref_count - 1
        WHERE part_id = ? AND day_utc = ?
          AND from_station = ? AND to_station = ? AND ref_count > 0
        """,
        (b["part_id"], b["day_utc"], a["station"], b["station"]),
    )
    if cur.rowcount == 0:
        raise RuntimeError(
            f"流向索引不一致, 缺少边: part={b['part_id']} day={b['day_utc']} "
            f"{a['station']}->{b['station']}"
        )
    conn.execute(
        """
        DELETE FROM flow_index
        WHERE part_id = ? AND day_utc = ?
          AND from_station = ? AND to_station = ? AND ref_count = 0
        """,
        (b["part_id"], b["day_utc"], a["station"], b["station"]),
    )


def _insert_scan_row(conn: sqlite3.Connection, scan_id: str, part_id: str,
                     station: str, ts_utc: str, day_utc: str) -> None:
    """在已开启的事务内插入一条扫码并增量更新流向 (只碰前后相邻)。"""
    prev = _prev_row(conn, part_id, ts_utc, scan_id)
    nxt = _next_row(conn, part_id, ts_utc, scan_id)

    # 新边把旧的 prev->next 邻接关系替换成 prev->new 与 new->next
    _remove_edge(conn, prev, nxt)
    conn.execute(
        "INSERT INTO scans (scan_id, part_id, ts_utc, day_utc, station)"
        " VALUES (?, ?, ?, ?, ?)",
        (scan_id, part_id, ts_utc, day_utc, station),
    )
    new_row = _get_scan(conn, scan_id)
    _add_edge(conn, prev, new_row)
    _add_edge(conn, new_row, nxt)


def _delete_scan_row(conn: sqlite3.Connection, scan_id: str) -> sqlite3.Row:
    """在已开启的事务内撤回一条扫码并增量修复流向。"""
    old = _get_scan(conn, scan_id)
    if old is None:
        raise NotFoundError(scan_id)
    prev = _prev_row(conn, old["part_id"], old["ts_utc"], old["scan_id"])
    nxt = _next_row(conn, old["part_id"], old["ts_utc"], old["scan_id"])

    _remove_edge(conn, prev, old)
    _remove_edge(conn, old, nxt)
    conn.execute("DELETE FROM scans WHERE scan_id = ?", (scan_id,))
    # old 被删后 prev 与 nxt 成为新的相邻扫码
    _add_edge(conn, prev, nxt)
    return old


# ---------------------------------------------------------------- 公开写接口

def add_scan(scan_id: str, part_id: str, station: str, ts_raw: str) -> dict[str, Any]:
    dt = parse_ts(ts_raw)
    ts_utc, day_utc = format_ts(dt), day_of(dt)
    conn = get_conn()
    with write_lock():
        try:
            conn.execute("BEGIN IMMEDIATE")
            if _get_scan(conn, scan_id) is not None:
                raise ConflictError(scan_id)
            _insert_scan_row(conn, scan_id, part_id, station, ts_utc, day_utc)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "scan_id": scan_id, "part_id": part_id, "station": station,
        "ts_utc": ts_utc, "day_utc": day_utc,
    }


def update_scan(scan_id: str, station: str | None, ts_raw: str | None) -> dict[str, Any]:
    """改时刻 / 改工位。仅传需要修改的字段。

    实现为同事务内"按旧位置撤回 + 按新位置插入", 每步都只查相邻扫码。
    """
    new_dt = parse_ts(ts_raw) if ts_raw is not None else None
    conn = get_conn()
    with write_lock():
        try:
            conn.execute("BEGIN IMMEDIATE")
            old = _get_scan(conn, scan_id)
            if old is None:
                raise NotFoundError(scan_id)

            part_id = old["part_id"]
            new_station = station if station is not None else old["station"]
            if new_dt is None:
                new_ts, new_day = old["ts_utc"], old["day_utc"]
            else:
                new_ts, new_day = format_ts(new_dt), day_of(new_dt)

            _delete_scan_row(conn, scan_id)
            _insert_scan_row(conn, scan_id, part_id, new_station, new_ts, new_day)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "scan_id": scan_id, "part_id": part_id, "station": new_station,
        "ts_utc": new_ts, "day_utc": new_day,
    }


def withdraw_scan(scan_id: str) -> dict[str, Any]:
    conn = get_conn()
    with write_lock():
        try:
            conn.execute("BEGIN IMMEDIATE")
            old = _delete_scan_row(conn, scan_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "scan_id": old["scan_id"], "part_id": old["part_id"],
        "station": old["station"], "ts_utc": old["ts_utc"],
        "day_utc": old["day_utc"], "withdrawn": True,
    }


# ---------------------------------------------------------------- 查询 (只读索引)

def _validate_day(value: str, field: str) -> str:
    try:
        return datetime.strptime(value, DAY_FMT).strftime(DAY_FMT)
    except ValueError as exc:
        raise ValueError(f"{field} 必须是 YYYY-MM-DD 格式: {value!r}") from exc


def query_flows(start_day: str, end_day: str) -> list[dict[str, Any]]:
    """按 UTC 日期区间统计各流向的工件数。

    只读 flow_index: 同件在区间内走同一流向只算一次 (主键天然去重,
    同日重复走同一流向时索引行数不随次数增长)。跨天则各天分别计一次。
    """
    start_day = _validate_day(start_day, "start_day")
    end_day = _validate_day(end_day, "end_day")
    if start_day > end_day:
        raise ValueError("start_day 不能晚于 end_day")
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT from_station, to_station,
               COUNT(DISTINCT part_id) AS part_count,
               SUM(ref_count) AS event_count
        FROM flow_index
        WHERE day_utc BETWEEN ? AND ?
        GROUP BY from_station, to_station
        ORDER BY from_station, to_station
        """,
        (start_day, end_day),
    ).fetchall()
    return [
        {
            "from_station": r["from_station"],
            "to_station": r["to_station"],
            "part_count": r["part_count"],
            "event_count": r["event_count"],
        }
        for r in rows
    ]


def list_scans(part_id: str | None = None) -> list[dict[str, Any]]:
    conn = get_conn()
    if part_id is None:
        rows = conn.execute(
            "SELECT * FROM scans ORDER BY part_id, ts_utc, scan_id"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM scans WHERE part_id = ? ORDER BY ts_utc, scan_id",
            (part_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def list_flow_index(part_id: str | None = None) -> list[dict[str, Any]]:
    """暴露索引内容, 便于核对 (同样是只读查询)。"""
    conn = get_conn()
    if part_id is None:
        rows = conn.execute(
            "SELECT * FROM flow_index ORDER BY day_utc, part_id, from_station, to_station"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM flow_index WHERE part_id = ?"
            " ORDER BY day_utc, from_station, to_station",
            (part_id,),
        ).fetchall()
    return [dict(r) for r in rows]
