#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ima 同步控制台 (SQLite 存储版)

数据全部存 SQLite (同目录 ima_sync.db):
  - config     全局凭据/口令
  - tasks      同步任务(多文件夹, 各自时间/目标/过滤/启停)
  - files      文件级状态与记录(路径/大小/mtime/sha256/media_id/结果)
  - runs       任务执行历史

功能:
  - 多文件夹独立任务, 到点自动同步(串行), 手动立即同步/试运行
  - 内容变化检测: sha256 变了即视为新版本, 自动重新上传
  - 同步记录: 全量查询 + 关键词搜索 + 任务/状态筛选 + 分页
  - 仪表盘统计、运行日志

运行: python3 webapp.py [--port 8765]
依赖: 仅 Python 3.7+ 标准库。上传逻辑复用同目录 ima_sync.py。
"""

import argparse
import collections
import datetime
import hmac
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

import ima_sync  # noqa: E402

# 数据文件目录: 容器内可通过 IMA_DB_DIR 指到挂载卷, 默认与脚本同目录
DATA_DIR = os.environ.get("IMA_DB_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, "ima_sync.db")
MAX_LOGS = 500
MAX_TASKS = 20

LOGS = collections.deque(maxlen=MAX_LOGS)
_LOG_LOCK = threading.Lock()

DB_LOCK = threading.RLock()


def log(msg):
    line = "%s %s" % (time.strftime("[%Y-%m-%d %H:%M:%S]"), msg)
    print(line, flush=True)
    with _LOG_LOCK:
        LOGS.append({"t": int(time.time()), "m": msg})


# ---------------------------------------------------------------- 数据库

def db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    with DB_LOCK:
        conn = db()
        conn.executescript("""CREATE TABLE IF NOT EXISTS config (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS tasks (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  name        TEXT NOT NULL,
  sync_dir    TEXT NOT NULL DEFAULT '',
  kb_id       TEXT NOT NULL DEFAULT '',
  kb_name     TEXT NOT NULL DEFAULT '',
  folder_id   TEXT NOT NULL DEFAULT '',
  folder_name TEXT NOT NULL DEFAULT '',
  sync_time   TEXT NOT NULL DEFAULT '00:30',
  ext         TEXT NOT NULL DEFAULT '',
  enabled     INTEGER NOT NULL DEFAULT 1,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  rel_path   TEXT NOT NULL,
  file_name  TEXT NOT NULL,
  ext        TEXT NOT NULL DEFAULT '',
  size       INTEGER NOT NULL DEFAULT 0,
  mtime      INTEGER NOT NULL DEFAULT 0,
  sha256     TEXT NOT NULL DEFAULT '',
  media_id   TEXT NOT NULL DEFAULT '',
  status     TEXT NOT NULL DEFAULT 'pending',   -- pending/synced/skipped/failed
  detail     TEXT NOT NULL DEFAULT '',
  updated_at INTEGER NOT NULL,
  UNIQUE(task_id, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_files_task ON files(task_id);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);

CREATE TABLE IF NOT EXISTS runs (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    INTEGER,
  task_name  TEXT NOT NULL DEFAULT '',
  trigger    TEXT NOT NULL DEFAULT 'manual',
  dry        INTEGER NOT NULL DEFAULT 0,
  total      INTEGER NOT NULL DEFAULT 0,
  ok         INTEGER NOT NULL DEFAULT 0,
  fail       INTEGER NOT NULL DEFAULT 0,
  skipped    INTEGER NOT NULL DEFAULT 0,
  status     TEXT NOT NULL DEFAULT 'running',
  message    TEXT NOT NULL DEFAULT '',
  start_time TEXT NOT NULL DEFAULT '',
  end_time   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_runs_task ON runs(task_id);
""")
        # 历史遗留: 旧版试运行曾把待传文件预写为 pending, 这些记录与真实文件状态
        # 无关(真实待传以磁盘扫描为准), 启动时清掉避免统计虚高
        conn.execute("DELETE FROM files WHERE status='pending'")
        conn.commit()
        conn.close()
    _migrate_json_if_any()


def _migrate_json_if_any():
    """旧版 JSON 配置/历史一次性迁入 SQLite。"""
    old_cfg = os.path.join(DATA_DIR, "web_config.json")
    if not os.path.exists(old_cfg):
        legacy = os.path.join(BASE_DIR, "web_config.json")
        if os.path.exists(legacy):
            old_cfg = legacy
    if os.path.exists(old_cfg):
        try:
            with open(old_cfg, "r", encoding="utf-8") as f:
                saved = json.load(f)
            cfg = get_config()
            if not cfg.get("client_id") and saved.get("client_id"):
                cfg["client_id"] = saved.get("client_id", "")
                cfg["api_key"] = saved.get("api_key", "")
                cfg["access_token"] = saved.get("access_token", "")
                set_config(cfg)
            tasks = list_tasks()
            if not tasks and saved.get("tasks"):
                for t in saved["tasks"]:
                    add_task(t)
                log("已从 web_config.json 迁移 %d 个任务到 SQLite" % len(saved["tasks"]))
            elif not tasks and saved.get("sync_dir"):
                add_task({
                    "name": os.path.basename(saved["sync_dir"]) or "我的同步",
                    "sync_dir": saved["sync_dir"],
                    "kb_id": saved.get("kb_id", ""), "kb_name": saved.get("kb_name", ""),
                    "folder_id": saved.get("folder_id", ""), "folder_name": saved.get("folder_name", ""),
                    "sync_time": saved.get("sync_time", "00:30"), "ext": "", "enabled": True,
                })
                log("已迁移旧版单目录配置到 SQLite")
            os.replace(old_cfg, old_cfg + ".migrated")
        except Exception as e:
            log("JSON 配置迁移失败: %s" % e)

    old_hist = os.path.join(DATA_DIR, "sync_history.json")
    if not os.path.exists(old_hist):
        legacy = os.path.join(BASE_DIR, "sync_history.json")
        if os.path.exists(legacy):
            old_hist = legacy
    if os.path.exists(old_hist):
        try:
            with open(old_hist, "r", encoding="utf-8") as f:
                data = json.load(f)
            conn = db()
            n = 0
            for r in reversed(data.get("runs") or []):
                conn.execute(
                    "INSERT INTO runs(task_id, task_name, trigger, dry, total, ok, fail, skipped, status, message, start_time, end_time)"
                    " VALUES(NULL,?,?,?,?,?,?,?,?,?,?,?)",
                    (r.get("task_name", ""), r.get("trigger", "manual"), 1 if r.get("dry") else 0,
                     r.get("total", 0), r.get("ok", 0), r.get("fail", 0), 0,
                     r.get("status", "done"), r.get("message", ""),
                     r.get("start_time", ""), r.get("end_time", "")))
                n += 1
            conn.commit()
            conn.close()
            os.replace(old_hist, old_hist + ".migrated")
            log("已从 sync_history.json 迁移 %d 条运行记录" % n)
        except Exception as e:
            log("历史记录迁移失败: %s" % e)


def get_config():
    with DB_LOCK:
        conn = db()
        rows = conn.execute("SELECT key, value FROM config").fetchall()
        conn.close()
    cfg = {r["key"]: r["value"] for r in rows}
    return {"client_id": cfg.get("client_id", ""),
            "api_key": cfg.get("api_key", ""),
            "access_token": cfg.get("access_token", "")}


def set_config(cfg):
    with DB_LOCK:
        conn = db()
        for k, v in cfg.items():
            conn.execute("INSERT INTO config(key, value) VALUES(?,?)"
                         " ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, str(v)))
        conn.commit()
        conn.close()


def list_tasks():
    with DB_LOCK:
        conn = db()
        rows = conn.execute("SELECT * FROM tasks ORDER BY id").fetchall()
        conn.close()
    return [dict(r) for r in rows]


def get_task(task_id):
    with DB_LOCK:
        conn = db()
        r = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        conn.close()
    return dict(r) if r else None


def add_task(t):
    now = int(time.time())
    with DB_LOCK:
        conn = db()
        cur = conn.execute(
            "INSERT INTO tasks(name, sync_dir, kb_id, kb_name, folder_id, folder_name, sync_time, ext, enabled, created_at, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (t.get("name", ""), t.get("sync_dir", ""), t.get("kb_id", ""), t.get("kb_name", ""),
             t.get("folder_id", ""), t.get("folder_name", ""), t.get("sync_time", "00:30"),
             t.get("ext", ""), 1 if t.get("enabled", True) else 0, now, now))
        conn.commit()
        tid = cur.lastrowid
        conn.close()
    return tid


def update_task(task_id, t):
    now = int(time.time())
    with DB_LOCK:
        conn = db()
        conn.execute(
            "UPDATE tasks SET name=?, sync_dir=?, kb_id=?, kb_name=?, folder_id=?, folder_name=?,"
            " sync_time=?, ext=?, enabled=?, updated_at=? WHERE id=?",
            (t.get("name", ""), t.get("sync_dir", ""), t.get("kb_id", ""), t.get("kb_name", ""),
             t.get("folder_id", ""), t.get("folder_name", ""), t.get("sync_time", "00:30"),
             t.get("ext", ""), 1 if t.get("enabled", True) else 0, now, task_id))
        conn.commit()
        conn.close()


def delete_task(task_id):
    with DB_LOCK:
        conn = db()
        conn.execute("DELETE FROM files WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.commit()
        conn.close()


def replace_tasks(tasks):
    """整体保存任务列表(供弹层式前端全量提交)。"""
    with DB_LOCK:
        conn = db()
        keep = {t.get("id") for t in tasks if t.get("id")}
        existing = {r["id"] for r in conn.execute("SELECT id FROM tasks").fetchall()}
        for rid in existing - keep:
            conn.execute("DELETE FROM files WHERE task_id=?", (rid,))
            conn.execute("DELETE FROM tasks WHERE id=?", (rid,))
        for i, t in enumerate(tasks):
            t = normalize_task(t, i)
            now = int(time.time())
            if t.get("id") and t["id"] in existing:
                conn.execute(
                    "UPDATE tasks SET name=?, sync_dir=?, kb_id=?, kb_name=?, folder_id=?, folder_name=?,"
                    " sync_time=?, ext=?, enabled=?, updated_at=? WHERE id=?",
                    (t["name"], t["sync_dir"], t["kb_id"], t["kb_name"], t["folder_id"],
                     t["folder_name"], t["sync_time"], t["ext"], 1 if t["enabled"] else 0, now, t["id"]))
            else:
                conn.execute(
                    "INSERT INTO tasks(name, sync_dir, kb_id, kb_name, folder_id, folder_name, sync_time, ext, enabled, created_at, updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (t["name"], t["sync_dir"], t["kb_id"], t["kb_name"], t["folder_id"],
                     t["folder_name"], t["sync_time"], t["ext"], 1 if t["enabled"] else 0, now, now))
        conn.commit()
        conn.close()


def normalize_task(raw, idx=0):
    t = {
        "id": raw.get("id"),
        "name": str(raw.get("name") or "").strip() or ("任务%d" % (idx + 1)),
        "sync_dir": str(raw.get("sync_dir") or "").strip(),
        "kb_id": str(raw.get("kb_id") or "").strip(),
        "kb_name": str(raw.get("kb_name") or "").strip(),
        "folder_id": str(raw.get("folder_id") or "").strip(),
        "folder_name": str(raw.get("folder_name") or "").strip(),
        "sync_time": str(raw.get("sync_time") or "00:30").strip(),
        "ext": ",".join(sorted({e.strip().lower().lstrip(".") for e in str(raw.get("ext") or "").split(",") if e.strip()})),
        "enabled": bool(raw.get("enabled", True)),
    }
    return t


def task_ext_set(task):
    if not task.get("ext"):
        return None
    return {"." + e for e in task["ext"].split(",") if e}


def upsert_file(task_id, rel_path, **fields):
    now = int(time.time())
    with DB_LOCK:
        conn = db()
        conn.execute(
            "INSERT INTO files(task_id, rel_path, file_name, ext, size, mtime, sha256, media_id, status, detail, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(task_id, rel_path) DO UPDATE SET "
            " file_name=excluded.file_name, ext=excluded.ext, size=excluded.size, mtime=excluded.mtime,"
            " sha256=excluded.sha256, media_id=excluded.media_id, status=excluded.status,"
            " detail=excluded.detail, updated_at=excluded.updated_at",
            (task_id, rel_path, os.path.basename(rel_path),
             os.path.splitext(rel_path)[1].lower().lstrip("."),
             fields.get("size", 0), fields.get("mtime", 0), fields.get("sha256", ""),
             fields.get("media_id", ""), fields.get("status", "pending"),
             fields.get("detail", ""), now))
        conn.commit()
        conn.close()


def insert_run(task_id, task_name, trigger, dry):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with DB_LOCK:
        conn = db()
        cur = conn.execute(
            "INSERT INTO runs(task_id, task_name, trigger, dry, status, start_time)"
            " VALUES(?,?,?,?, 'running', ?)",
            (task_id, task_name, trigger, 1 if dry else 0, now))
        conn.commit()
        rid = cur.lastrowid
        conn.close()
    return rid


def finish_run(run_id, total, ok, fail, skipped, status, message):
    with DB_LOCK:
        conn = db()
        conn.execute(
            "UPDATE runs SET total=?, ok=?, fail=?, skipped=?, status=?, message=?, end_time=? WHERE id=?",
            (total, ok, fail, skipped, status, message, time.strftime("%Y-%m-%d %H:%M:%S"), run_id))
        conn.commit()
        conn.close()


def prune_runs(keep=500):
    with DB_LOCK:
        conn = db()
        conn.execute("DELETE FROM runs WHERE id NOT IN"
                     " (SELECT id FROM runs ORDER BY id DESC LIMIT ?)", (keep,))
        conn.commit()
        conn.close()


# ---------------------------------------------------------------- 同步执行

class SyncManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.current = None       # 当前运行描述
        self.queue = []           # 待执行 task_id 队列
        self._pending_trigger = "manual"
        self._pending_dry = False
        self._pending_limit = 0
        threading.Thread(target=self._worker, daemon=True).start()

    def start(self, task_ids, trigger="manual", dry=False, limit=0):
        with self.lock:
            if self.current:
                return False, "已有同步任务在运行中"
            cfg = get_config()
            tasks = []
            problems = []
            for tid in task_ids:
                t = get_task(tid)
                if not t:
                    continue
                if not t["sync_dir"]:
                    problems.append("「%s」未填写文件夹路径" % t["name"])
                elif not os.path.isdir(t["sync_dir"]):
                    problems.append("「%s」文件夹不存在: %s" % (t["name"], t["sync_dir"]))
                if not cfg.get("client_id"):
                    problems.append("请先在设置里填写 Client ID 和 API Key")
                    break
                if not t["kb_id"]:
                    problems.append("「%s」未选择知识库" % t["name"])
                tasks.append(t)
            if not tasks:
                return False, "没有可执行的任务"
            if problems:
                return False, "；".join(problems)
            self.queue = [t["id"] for t in tasks]
            self._pending_trigger = trigger
            self._pending_dry = dry
            self._pending_limit = max(int(limit or 0), 0)
            self.current = None
            return True, "已开始"

    def _worker(self):
        while True:
            time.sleep(0.5)
            with self.lock:
                if not self.queue:
                    self.current = None
                    continue
                if not self.current:
                    first = get_task(self.queue[0])
                    self.current = {"task_id": self.queue[0],
                                    "task_name": first["name"] if first else "",
                                    "trigger": self._pending_trigger, "dry": self._pending_dry,
                                    "limit": self._pending_limit,
                                    "total": 0, "done": 0, "ok": 0, "fail": 0, "skipped": 0,
                                    "current_file": "", "status": "running"}
                task_id = self.queue.pop(0)
                task = get_task(task_id)
                if not task:
                    continue
                self.current.update({"task_id": task_id, "task_name": task["name"],
                                     "total": 0, "done": 0, "ok": 0, "fail": 0, "skipped": 0,
                                     "current_file": "", "status": "running"})
            try:
                self._run_one(task, self.current)
            except Exception as e:
                self.current["status"] = "error"
                self.current["message"] = "任务异常: %s" % e
                log("任务「%s」异常: %s" % (task["name"], e))
                finish_run(self.current.get("run_id", 0), 0, 0, 0, 0, "error",
                           "任务异常: %s" % e)
            with self.lock:
                if not self.queue:
                    self.current = None

    def _run_one(self, task, cur):
        dry = cur["dry"]
        root = os.path.abspath(task["sync_dir"])
        cfg = get_config()
        client_id = cfg.get("client_id", "").strip()
        api_key = cfg.get("api_key", "").strip()
        kb_id = task["kb_id"]
        folder_id = task["folder_id"]
        include_ext = task_ext_set(task)
        run_id = insert_run(task["id"], task["name"], cur["trigger"], dry)
        cur["run_id"] = run_id
        cur["start_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

        candidates = ima_sync.scan_files(root, include_ext=include_ext)
        todo = []
        with DB_LOCK:
            conn = db()
            rows = {r["rel_path"]: dict(r) for r in conn.execute(
                "SELECT rel_path, size, mtime, sha256, status FROM files WHERE task_id=?",
                (task["id"],)).fetchall()}
            conn.close()
        for path in candidates:
            rel = os.path.relpath(path, root).replace("\\", "/")
            st = os.stat(path)
            prev = rows.get(rel)
            # 增量: 大小+mtime 相同则跳过; 否则算 hash, 与库里一致也跳过
            if prev and prev["size"] == st.st_size and prev["mtime"] == int(st.st_mtime):
                continue
            sha = ima_sync.file_hash(path)
            if prev and prev["sha256"] == sha and prev["status"] in ("synced", "skipped"):
                # 内容没变(只是 mtime/大小动了), 更新元数据即可
                upsert_file(task["id"], rel, size=st.st_size, mtime=int(st.st_mtime),
                            sha256=sha, media_id=prev.get("media_id", ""),
                            status=prev["status"], detail=prev.get("detail", ""))
                continue
            info = ima_sync.detect_file_type(path)
            if info and st.st_size > info[2]:
                log("「%s」跳过(超过大小上限): %s" % (task["name"], rel))
                upsert_file(task["id"], rel, size=st.st_size, mtime=int(st.st_mtime),
                            sha256=sha, status="failed", detail="超过大小上限")
                continue
            todo.append((path, rel, st.st_size, int(st.st_mtime), sha))

        cur["total"] = len(todo)
        limit = int(cur.get("limit") or 0)
        truncated = 0
        if limit > 0 and len(todo) > limit:
            truncated = len(todo) - limit
            todo = todo[:limit]
            cur["total"] = limit
        mode_txt = "限流试传(前%d个)" % limit if (limit > 0 and not dry) else ("试运行" if dry else "同步")
        log("「%s」%s开始: 待处理 %d 个文件%s (触发: %s)" % (
            task["name"], mode_txt, len(todo),
            "（其余 %d 个待下次同步）" % truncated if truncated else "", cur["trigger"]))
        if not todo:
            finish_run(run_id, 0, 0, 0, 0, "done", "没有新增或修改的文件")
            cur["status"] = "done"
            cur["message"] = "没有新增或修改的文件"
            return

        # 目录映射: 本地子目录层级原样映射到云端真实文件夹。
        # (实测: create_folder 带 folder_id 参数即在指定父目录下创建, 支持任意深度;
        #  add_knowledge 的 folder_id 引用已有文件夹有效)
        folder_cache = {}   # ('父目录id','名字') -> folder_id
        repeated_by_dir = {}
        if not dry:
            try:
                groups = {}
                for path, rel, size, mtime, sha in todo:
                    info = ima_sync.detect_file_type(path)
                    prev = rows.get(rel)
                    if not prev or not prev.get("media_id"):
                        if info:
                            groups.setdefault(os.path.dirname(rel), []).append(
                                {"name": os.path.basename(path), "media_type": info[0]})
                for rel_dir, items in groups.items():
                    parts = [p for p in rel_dir.split("/") if p]
                    try:
                        target_fid = ima_sync.ensure_folder_path(
                            kb_id, folder_id, parts, client_id, api_key, folder_cache)
                    except RuntimeError as e:
                        log("「%s」目录定位失败(%s), 该目录文件按根目录处理: %s" % (
                            task["name"], rel_dir or "/", e))
                        target_fid = folder_id or ""
                    try:
                        repeated_by_dir[rel_dir] = ima_sync.check_repeated(
                            items, kb_id, target_fid, client_id, api_key)
                    except RuntimeError as e:
                        log("「%s」检查重名失败(继续尝试上传): %s" % (task["name"], e))
                        repeated_by_dir[rel_dir] = set()
            except RuntimeError as e:
                log("「%s」检查重名失败(继续尝试上传): %s" % (task["name"], e))

        ok = fail = skipped = 0
        for path, rel, size, mtime, sha in todo:
            cur["current_file"] = rel
            name = os.path.basename(path)
            rel_dir = os.path.dirname(rel)
            parts = [p for p in rel_dir.split("/") if p]
            if dry:
                # 试运行只统计, 不写库(避免残留 pending 干扰统计)
                ok += 1
                log("「%s」[试运行] 将上传: %s" % (task["name"], rel))
            elif name in repeated_by_dir.get(rel_dir, set()):
                upsert_file(task["id"], rel, size=size, mtime=mtime, sha256=sha,
                            status="skipped", detail="ima 中已存在同名文件")
                skipped += 1
                log("「%s」跳过(ima中已存在同名): %s" % (task["name"], rel))
            else:
                # 本地子目录层级 -> 云端真实文件夹层级
                try:
                    target_fid = ima_sync.ensure_folder_path(
                        kb_id, folder_id, parts, client_id, api_key, folder_cache)
                    media_id = ima_sync.upload_one(path, kb_id, target_fid, client_id, api_key)
                    upsert_file(task["id"], rel, size=size, mtime=mtime, sha256=sha,
                                media_id=media_id, status="synced", detail="上传成功")
                    ok += 1
                    log("「%s」上传成功: %s" % (task["name"], rel))
                except Exception as e:
                    upsert_file(task["id"], rel, size=size, mtime=mtime, sha256=sha,
                                status="failed", detail=str(e))
                    fail += 1
                    log("「%s」上传失败: %s -> %s" % (task["name"], rel, e))
            cur["done"] += 1
            cur["ok"] = ok
            cur["fail"] = fail
            cur["skipped"] = skipped
            time.sleep(1)

        if limit > 0 and not dry:
            msg = "限流试传: 成功 %d, 跳过 %d, 失败 %d（已传前 %d 个%s）" % (
                ok, skipped, fail, limit, "，其余 %d 个待下次同步" % truncated if truncated else "")
        elif dry:
            msg = "试运行: 待上传 %d 个" % len(todo)
        else:
            msg = "成功 %d, 跳过 %d, 失败 %d" % (ok, skipped, fail)
        finish_run(run_id, len(todo), ok, fail, skipped, "done", msg)
        prune_runs()
        cur["status"] = "done"
        cur["message"] = msg
        log("「%s」%s" % (task["name"], msg))

    def snapshot(self):
        with self.lock:
            return dict(self.current) if self.current else None


MANAGER = SyncManager()


# ---------------------------------------------------------------- 调度器

class Scheduler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.fired = {}

    @staticmethod
    def parse_time(t):
        m = re.match(r"^(\d{1,2}):(\d{2})$", (t or "").strip())
        if not m:
            return None
        hh, mm = int(m.group(1)), int(m.group(2))
        if hh > 23 or mm > 59:
            return None
        return hh, mm

    def run(self):
        while True:
            try:
                now = datetime.datetime.now()
                for t in list_tasks():
                    if not t["enabled"]:
                        continue
                    tt = self.parse_time(t["sync_time"])
                    if not tt:
                        continue
                    fire = now.replace(hour=tt[0], minute=tt[1], second=0, microsecond=0)
                    key = (t["id"], now.date().isoformat())
                    if abs((now - fire).total_seconds()) < 15 and not self.fired.get(key):
                        self.fired[key] = True
                        log("任务「%s」到达计划时间 %s,自动开始同步" % (t["name"], t["sync_time"]))
                        ok, msg = MANAGER.start([t["id"]], trigger="schedule")
                        if not ok:
                            log("任务「%s」定时同步未启动: %s" % (t["name"], msg))
            except Exception as e:
                log("调度器异常: %s" % e)
            time.sleep(15)


def next_run_time(t):
    tt = Scheduler.parse_time(t["sync_time"])
    if not tt:
        return None
    now = datetime.datetime.now()
    target = now.replace(hour=tt[0], minute=tt[1], second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    return target.strftime("%Y-%m-%d %H:%M")


# ---------------------------------------------------------------- ima 辅助

def ima_list_knowledge_bases(client_id, api_key):
    data = ima_sync.http_post_json(
        ima_sync.OPENAPI_PATH + "/get_addable_knowledge_base_list",
        {"cursor": "", "limit": 50}, client_id, api_key)
    return data.get("addable_knowledge_base_list") or []


def ima_list_folders(kb_id, client_id, api_key):
    data = ima_sync.http_post_json(
        ima_sync.OPENAPI_PATH + "/get_knowledge_list",
        {"cursor": "", "limit": 50, "knowledge_base_id": kb_id},
        client_id, api_key)
    folders = []
    for key in ("folder_list", "folder_info_list", "folders"):
        for f in data.get(key) or []:
            folders.append({"folder_id": f.get("folder_id", ""),
                            "name": f.get("name", ""),
                            "file_number": f.get("file_number", 0)})
    if not folders:
        for item in data.get("knowledge_list") or []:
            if item.get("folder_id"):
                folders.append({"folder_id": item["folder_id"],
                                "name": item.get("title", ""), "file_number": 0})
    return folders


# ---------------------------------------------------------------- HTTP

INDEX_FILE = os.path.join(BASE_DIR, "index.html")


class Handler(BaseHTTPRequestHandler):
    server_version = "ImaSyncWeb/3.0"

    def log_message(self, fmt, *args):
        pass

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self):
        try:
            with open(INDEX_FILE, "rb") as f:
                body = f.read()
        except OSError:
            self._send_json({"error": "index.html 缺失"}, 500)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        token = get_config().get("access_token", "")
        if not token:
            return True
        given = self.headers.get("X-Access-Token", "")
        if not given and "token=" in self.path:
            given = self.path.split("token=")[-1].split("&")[0]
        return hmac.compare_digest(given, token)

    def _guard(self):
        if not self._check_auth():
            self._send_json({"error": "需要访问口令"}, 401)
            return False
        return True

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > 1024 * 1024:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            return {}

    # ---- GET ----
    def do_GET(self):
        path = self.path.split("?")[0]
        qs = {}
        m = re.findall(r"([^?&=]+)=([^&]*)", self.path)
        for k, v in m:
            qs[k] = urllib.request.unquote(v)
        if path in ("/", "/index.html"):
            self._send_file()
            return
        if not self._guard():
            return
        if path == "/api/status":
            cfg = get_config()
            cur = MANAGER.snapshot()
            tasks = []
            totals = {"total": 0, "synced": 0, "pending": 0, "failed": 0}
            with DB_LOCK:
                conn = db()
                for t in list_tasks():
                    row = conn.execute(
                        "SELECT status, COUNT(*) AS n FROM files WHERE task_id=? GROUP BY status",
                        (t["id"],)).fetchall()
                    c = {r["status"]: r["n"] for r in row}
                    total = sum(c.values())
                    synced = c.get("synced", 0) + c.get("skipped", 0)
                    pending = c.get("pending", 0)
                    failed = c.get("failed", 0)
                    totals["total"] += total
                    totals["synced"] += synced
                    totals["pending"] += pending
                    totals["failed"] += failed
                    tasks.append({
                        "id": t["id"], "name": t["name"], "sync_dir": t["sync_dir"],
                        "kb_name": t["kb_name"] or t["kb_id"], "sync_time": t["sync_time"],
                        "ext": t["ext"], "enabled": bool(t["enabled"]),
                        "next_run": next_run_time(t),
                        "stats": {"total": total, "synced": synced,
                                  "pending": pending, "failed": failed},
                    })
                last = conn.execute("SELECT * FROM runs WHERE status!='running' ORDER BY id DESC LIMIT 1").fetchone()
            self._send_json({
                "running": bool(cur),
                "current": cur,
                "last_run": dict(last) if last else None,
                "tasks": tasks,
                "totals": totals,
                "config_ready": bool(cfg.get("client_id") and cfg.get("api_key")),
            })
        elif path == "/api/config":
            cfg = get_config()
            cfg["tasks"] = list_tasks()
            self._send_json(cfg)
        elif path == "/api/files":
            # 文件记录查询: q(路径/文件名模糊) task_id status page page_size
            q = qs.get("q", "").strip()
            task_id = qs.get("task_id", "").strip()
            status = qs.get("status", "").strip()
            page = max(int(qs.get("page", "1") or 1), 1)
            page_size = min(max(int(qs.get("page_size", "50") or 50), 1), 200)
            where, args = [], []
            if q:
                where.append("(f.rel_path LIKE ? OR f.media_id LIKE ?)")
                args += ["%%%s%%" % q, "%%%s%%" % q]
            if task_id:
                where.append("f.task_id=?")
                args.append(int(task_id))
            if status:
                where.append("f.status=?")
                args.append(status)
            wsql = ("WHERE " + " AND ".join(where)) if where else ""
            with DB_LOCK:
                conn = db()
                n = conn.execute("SELECT COUNT(*) AS n FROM files f %s" % wsql, args).fetchone()["n"]
                rows = conn.execute(
                    "SELECT f.*, t.name AS task_name, t.sync_dir FROM files f"
                    " LEFT JOIN tasks t ON t.id=f.task_id %s"
                    " ORDER BY f.updated_at DESC, f.id DESC LIMIT ? OFFSET ?" % wsql,
                    args + [page_size, (page - 1) * page_size]).fetchall()
                conn.close()
            self._send_json({"total": n, "page": page, "page_size": page_size,
                             "files": [dict(r) for r in rows]})
        elif path == "/api/runs":
            page = max(int(qs.get("page", "1") or 1), 1)
            page_size = min(max(int(qs.get("page_size", "50") or 50), 1), 200)
            with DB_LOCK:
                conn = db()
                n = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"]
                rows = conn.execute(
                    "SELECT * FROM runs ORDER BY id DESC LIMIT ? OFFSET ?",
                    (page_size, (page - 1) * page_size)).fetchall()
                conn.close()
            self._send_json({"total": n, "page": page, "page_size": page_size,
                             "runs": [dict(r) for r in rows]})
        elif path == "/api/browse":
            # 目录浏览: ?path=/vol1  返回子目录列表(只读, 用于前端可视化选择文件夹)
            drive_root = os.path.abspath(os.sep)
            raw = (qs.get("path") or "").strip()
            # 只允许浏览绝对路径; 相对路径一律回落到根
            cur = os.path.abspath(raw) if (raw and os.path.isabs(raw)) else drive_root
            parent = os.path.dirname(cur)
            entries = []
            try:
                with os.scandir(cur) as it:
                    for e in it:
                        try:
                            if e.is_dir(follow_symlinks=False) and not e.name.startswith("."):
                                entries.append(e.name)
                        except OSError:
                            continue
            except (OSError, PermissionError) as e:
                self._send_json({"ok": False, "error": "无法读取该目录: %s" % e,
                                 "current": cur, "parent": parent if parent != cur else None,
                                 "dirs": []})
                return
            entries.sort(key=str.lower)
            self._send_json({"ok": True, "current": cur,
                             "parent": parent if parent != cur else None,
                             "dirs": entries})
        elif path == "/api/logs":
            with _LOG_LOCK:
                logs = list(LOGS)
            self._send_json({"logs": logs})
        else:
            self._send_json({"error": "not found"}, 404)

    # ---- POST ----
    def do_POST(self):
        path = self.path.split("?")[0]
        cfg = get_config()
        if path == "/api/config":
            if cfg.get("access_token") and not self._check_auth():
                self._send_json({"error": "需要访问口令"}, 401)
                return
        elif not self._guard():
            return
        body = self._read_body()

        if path == "/api/config":
            for key in ("client_id", "api_key", "access_token"):
                if key in body:
                    cfg[key] = str(body[key]).strip()
            set_config(cfg)
            if "tasks" in body:
                if not isinstance(body["tasks"], list) or len(body["tasks"]) > MAX_TASKS:
                    self._send_json({"error": "tasks 应为不超过 %d 个的列表" % MAX_TASKS}, 400)
                    return
                for t in body["tasks"]:
                    nt = normalize_task(t)
                    if not Scheduler.parse_time(nt["sync_time"]):
                        self._send_json({"error": "任务「%s」的同步时间格式应为 HH:MM" % nt["name"]}, 400)
                        return
                replace_tasks(body["tasks"])
            log("配置已保存")
            self._send_json({"ok": True})
        elif path == "/api/tasks/add":
            nt = normalize_task(body, len(list_tasks()))
            if not nt["sync_dir"]:
                self._send_json({"error": "文件夹路径不能为空"}, 400)
                return
            if not Scheduler.parse_time(nt["sync_time"]):
                self._send_json({"error": "同步时间格式应为 HH:MM"}, 400)
                return
            tid = add_task(nt)
            log("新建任务「%s」(id=%d)" % (nt["name"], tid))
            self._send_json({"ok": True, "id": tid})
        elif path == "/api/tasks/update":
            tid = int(body.get("id") or 0)
            t = get_task(tid)
            if not t:
                self._send_json({"error": "任务不存在"}, 404)
                return
            nt = normalize_task({**t, **body})
            if not Scheduler.parse_time(nt["sync_time"]):
                self._send_json({"error": "同步时间格式应为 HH:MM"}, 400)
                return
            update_task(tid, nt)
            log("更新任务「%s」(id=%d)" % (nt["name"], tid))
            self._send_json({"ok": True})
        elif path == "/api/tasks/delete":
            tid = int(body.get("id") or 0)
            t = get_task(tid)
            if not t:
                self._send_json({"error": "任务不存在"}, 404)
                return
            delete_task(tid)
            log("删除任务「%s」(id=%d)" % (t["name"], tid))
            self._send_json({"ok": True})
        elif path == "/api/tasks/toggle":
            tid = int(body.get("id") or 0)
            t = get_task(tid)
            if not t:
                self._send_json({"error": "任务不存在"}, 404)
                return
            update_task(tid, {**t, "enabled": not t["enabled"]})
            self._send_json({"ok": True, "enabled": not t["enabled"]})
        elif path == "/api/sync/run":
            wanted = body.get("tasks")
            all_tasks = list_tasks()
            if wanted is None:
                ids = [t["id"] for t in all_tasks if t["enabled"]]
            else:
                by_name = {t["name"]: t["id"] for t in all_tasks}
                by_id = {t["id"]: t["id"] for t in all_tasks}
                ids = [by_name.get(w) or by_id.get(w) for w in wanted
                       if (by_name.get(w) or by_id.get(w))]
            if wanted is not None and not ids:
                self._send_json({"ok": False, "message": "没有匹配的任务"})
                return
            try:
                limit = max(int(body.get("limit") or 0), 0)
            except (TypeError, ValueError):
                limit = 0
            ok, msg = MANAGER.start(ids, trigger="manual",
                                    dry=bool(body.get("dry")), limit=limit)
            self._send_json({"ok": ok, "message": msg})
        elif path == "/api/ima/test":
            cid = (body.get("client_id") or cfg.get("client_id", "")).strip()
            key = (body.get("api_key") or cfg.get("api_key", "")).strip()
            if not cid or not key:
                self._send_json({"ok": False, "error": "Client ID / API Key 不能为空"})
                return
            try:
                self._send_json({"ok": True, "knowledge_bases": ima_list_knowledge_bases(cid, key)})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)})
        elif path == "/api/ima/folders":
            cid = (body.get("client_id") or cfg.get("client_id", "")).strip()
            key = (body.get("api_key") or cfg.get("api_key", "")).strip()
            kb_id = (body.get("kb_id") or "").strip()
            if not (cid and key and kb_id):
                self._send_json({"ok": False, "error": "需要 Client ID / API Key / 知识库ID"})
                return
            try:
                self._send_json({"ok": True, "folders": ima_list_folders(kb_id, cid, key)})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)})
        else:
            self._send_json({"error": "not found"}, 404)


def main():
    parser = argparse.ArgumentParser(description="ima 同步控制台")
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("IMA_WEB_PORT", "8765")))
    args = parser.parse_args()

    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.daemon_threads = True
    Scheduler().start()
    log("同步控制台已启动: http://0.0.0.0:%d  (SQLite: %s)" % (args.port, DB_FILE))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("服务停止")


if __name__ == "__main__":
    main()
