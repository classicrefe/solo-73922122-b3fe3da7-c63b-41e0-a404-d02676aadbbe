"""FastAPI 入口: 扫码录入/修改/撤回 + 流向统计, 文档见 /docs。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, status

from . import services
from .db import init_db
from .schemas import (
    FlowCount,
    FlowIndexRow,
    ScanCreate,
    ScanOut,
    ScanUpdate,
    WithdrawOut,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="返工补登扫码流向核对服务",
    description=(
        "接收工位扫码并维护相邻工位流向索引。"
        "同件扫码按 (UTC 时刻, 扫码编号) 排序, 相邻工位不同才产生流向, "
        "归入后一次扫码的 UTC 日期。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/scans", response_model=ScanOut, status_code=status.HTTP_201_CREATED,
          summary="录入一次扫码")
def create_scan(payload: ScanCreate) -> ScanOut:
    try:
        return ScanOut(**services.add_scan(
            payload.scan_id, payload.part_id, payload.station, payload.ts_utc
        ))
    except services.ConflictError as exc:
        raise HTTPException(status_code=409,
                            detail=f"scan_id 已存在: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.patch("/scans/{scan_id}", response_model=ScanOut,
           summary="改时刻 / 改工位 (仅传需要改的字段)")
def patch_scan(scan_id: str, payload: ScanUpdate) -> ScanOut:
    if payload.station is None and payload.ts_utc is None:
        raise HTTPException(status_code=422,
                            detail="station 与 ts_utc 至少要传一个")
    try:
        return ScanOut(**services.update_scan(
            scan_id, payload.station, payload.ts_utc
        ))
    except services.NotFoundError as exc:
        raise HTTPException(status_code=404,
                            detail=f"scan_id 不存在: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.delete("/scans/{scan_id}", response_model=WithdrawOut,
            summary="撤回一次扫码")
def delete_scan(scan_id: str) -> WithdrawOut:
    try:
        return WithdrawOut(**services.withdraw_scan(scan_id))
    except services.NotFoundError as exc:
        raise HTTPException(status_code=404,
                            detail=f"scan_id 不存在: {exc}") from exc


@app.get("/scans", response_model=list[ScanOut], summary="查询扫码明细")
def get_scans(part_id: str | None = Query(None, description="按工件号过滤")) -> list[ScanOut]:
    return [ScanOut(**r) for r in services.list_scans(part_id)]


@app.get("/flows", response_model=list[FlowCount],
         summary="按 UTC 日期区间统计各流向工件数 (只读流向索引)")
def get_flows(
    start_day: str = Query(..., description="起始 UTC 日期 YYYY-MM-DD"),
    end_day: str = Query(..., description="结束 UTC 日期 YYYY-MM-DD"),
) -> list[FlowCount]:
    try:
        return [FlowCount(**r) for r in services.query_flows(start_day, end_day)]
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/flow-index", response_model=list[FlowIndexRow],
         summary="查看流向索引明细 (核对用)")
def get_flow_index(
    part_id: str | None = Query(None, description="按工件号过滤"),
) -> list[FlowIndexRow]:
    return [FlowIndexRow(**r) for r in services.list_flow_index(part_id)]
