"""SQLite 持久化：计划模板、运行记录、指标时序。

标准库 sqlite3 实现，所有数据落在工作目录 .chaoslab/chaoslab.db。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any

from chaoslab.model import DrillPlan, RunRecord, plan_from_dict, to_jsonable


class Store:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS plan_templates (
                    name TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    plan_name TEXT NOT NULL,
                    resilience_mode TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    finished_at REAL NOT NULL,
                    status TEXT NOT NULL,
                    abort_reason TEXT,
                    summary TEXT,
                    steady_checks TEXT,
                    events TEXT,
                    notes TEXT
                );
                CREATE TABLE IF NOT EXISTS metric_series (
                    run_id TEXT NOT NULL,
                    ts REAL NOT NULL,
                    service TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_series_run
                    ON metric_series(run_id, ts);
                """)

    # ------------------------------------------------ 计划模板
    def save_plan_template(self, plan: DrillPlan) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO plan_templates(name, content, updated_at) "
                "VALUES (?, ?, ?)",
                (plan.name, json.dumps(to_jsonable(plan), ensure_ascii=False),
                 time.time()))

    def load_plan_template(self, name: str) -> DrillPlan | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT content FROM plan_templates WHERE name=?", (name,)).fetchone()
        return plan_from_dict(json.loads(row["content"])) if row else None

    def list_plan_templates(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name, updated_at FROM plan_templates ORDER BY updated_at DESC"
            ).fetchall()
        return [{"name": r["name"], "updated_at": r["updated_at"]} for r in rows]

    def delete_plan_template(self, name: str) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM plan_templates WHERE name=?", (name,))
        return cur.rowcount > 0

    # ------------------------------------------------ 运行记录
    def save_run(self, run: RunRecord) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO runs(run_id, plan_name, resilience_mode, "
                "started_at, finished_at, status, abort_reason, summary, "
                "steady_checks, events, notes) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (run.run_id, run.plan_name, run.resilience_mode, run.started_at,
                 run.finished_at, run.status, run.abort_reason,
                 json.dumps(run.summary, ensure_ascii=False),
                 json.dumps(run.steady_checks, ensure_ascii=False),
                 json.dumps(run.events, ensure_ascii=False), run.notes))

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE run_id=?",
                                     (run_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        for key in ("summary", "steady_checks", "events"):
            d[key] = json.loads(d[key] or "{}" if key == "summary"
                                else d[key] or "[]")
        return d

    def list_runs(self, plan_name: str = "") -> list[dict[str, Any]]:
        with self._lock:
            if plan_name:
                rows = self._conn.execute(
                    "SELECT run_id, plan_name, resilience_mode, started_at, "
                    "finished_at, status FROM runs WHERE plan_name=? "
                    "ORDER BY started_at DESC", (plan_name,)).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT run_id, plan_name, resilience_mode, started_at, "
                    "finished_at, status FROM runs ORDER BY started_at DESC"
                ).fetchall()
        return [dict(r) for r in rows]

    def latest_two_runs(self, plan_name: str) -> list[dict[str, Any]]:
        """取同一计划最近两次 run（通常就是 off / on 各一次，用于对比）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id, plan_name, resilience_mode, started_at, status "
                "FROM runs WHERE plan_name=? ORDER BY started_at DESC LIMIT 2",
                (plan_name,)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------ 指标时序
    def append_metric(self, run_id: str, ts: float, service: str,
                      payload: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO metric_series(run_id, ts, service, payload) "
                "VALUES (?, ?, ?, ?)",
                (run_id, ts, service, json.dumps(payload, ensure_ascii=False)))

    def get_series(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, service, payload FROM metric_series "
                "WHERE run_id=? ORDER BY ts", (run_id,)).fetchall()
        out = []
        for r in rows:
            item = {"ts": r["ts"], "service": r["service"]}
            item.update(json.loads(r["payload"]))
            out.append(item)
        return out

    def close(self) -> None:
        with self._lock:
            self._conn.close()
