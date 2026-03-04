import argparse
import json
import os
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib import request, parse


WRITE_TOKENS = {"insert", "update", "delete", "replace", "create", "alter", "drop"}
READ_TOKENS = {"select", "pragma", "explain"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def strip_leading_comments(sql: str) -> str:
    s = sql
    while True:
        s = s.lstrip()
        if s.startswith("--"):
            i = s.find("\n")
            if i == -1:
                return ""
            s = s[i + 1 :]
            continue
        if s.startswith("/*"):
            i = s.find("*/")
            if i == -1:
                return ""
            s = s[i + 2 :]
            continue
        return s


def first_sql_token(sql: str) -> str:
    s = strip_leading_comments(sql)
    m = re.match(r"([A-Za-z]+)", s)
    return m.group(1).lower() if m else ""


def classify_sql(sql: str) -> str:
    token = first_sql_token(sql)
    if token in WRITE_TOKENS:
        return "write"
    if token in READ_TOKENS:
        return "read"
    return "unsupported"


def normalize_statements(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "statements" in payload:
        statements = payload["statements"]
        if not isinstance(statements, list) or not statements:
            raise ValueError("'statements' must be a non-empty list")
        normalized = []
        for i, stmt in enumerate(statements):
            if not isinstance(stmt, dict):
                raise ValueError(f"statement #{i} must be an object")
            sql = stmt.get("sql")
            params = stmt.get("params", [])
            if not isinstance(sql, str) or not sql.strip():
                raise ValueError(f"statement #{i} missing valid 'sql'")
            if not isinstance(params, (list, tuple)):
                raise ValueError(f"statement #{i} 'params' must be a list")
            normalized.append({"sql": sql, "params": list(params)})
        return normalized

    if "sql" in payload:
        sql = payload["sql"]
        params = payload.get("params", [])
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("'sql' must be a non-empty string")
        if not isinstance(params, (list, tuple)):
            raise ValueError("'params' must be a list")
        return [{"sql": sql, "params": list(params)}]

    raise ValueError("body must contain either 'sql' or 'statements'")


class SQLiteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.write_lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _distdb_applied_ops (
                    op_id TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS _distdb_op_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    op_id TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def query(self, sql: str, params: List[Any]) -> Dict[str, Any]:
        if classify_sql(sql) != "read":
            raise ValueError("only read SQL is allowed on /query (SELECT/PRAGMA/EXPLAIN)")

        with self._connect() as conn:
            cur = conn.execute(sql, tuple(params))
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description] if cur.description else []
            return {
                "columns": columns,
                "rows": [dict(r) for r in rows],
                "rowcount": len(rows),
            }

    def apply_write_transaction(
        self,
        op_id: str,
        statements: List[Dict[str, Any]],
        record_op_log: bool,
        payload_for_log: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        for i, stmt in enumerate(statements):
            kind = classify_sql(stmt["sql"])
            if kind != "write":
                raise ValueError(
                    f"statement #{i} is not a supported write statement "
                    f"(allowed starts: {sorted(WRITE_TOKENS)})"
                )

        with self.write_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")

                existing = conn.execute(
                    "SELECT 1 FROM _distdb_applied_ops WHERE op_id = ?",
                    (op_id,),
                ).fetchone()

                if existing:
                    conn.rollback()
                    return {
                        "applied": False,
                        "duplicate": True,
                        "results": [],
                    }

                results = []
                for stmt in statements:
                    cur = conn.execute(stmt["sql"], tuple(stmt["params"]))
                    results.append(
                        {
                            "rowcount": cur.rowcount,
                            "lastrowid": cur.lastrowid,
                        }
                    )

                now = utc_now()
                conn.execute(
                    "INSERT INTO _distdb_applied_ops (op_id, applied_at) VALUES (?, ?)",
                    (op_id, now),
                )

                if record_op_log:
                    if payload_for_log is None:
                        raise ValueError("payload_for_log is required when record_op_log=True")
                    conn.execute(
                        """
                        INSERT INTO _distdb_op_log (op_id, payload_json, created_at)
                        VALUES (?, ?, ?)
                        """,
                        (op_id, json.dumps(payload_for_log), now),
                    )

                conn.commit()
                return {
                    "applied": True,
                    "duplicate": False,
                    "results": results,
                }
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()


class ReplicatedNode:
    def __init__(self, node_id: str, role: str, port: int, db_path: str, peers: List[str]):
        self.node_id = node_id
        self.role = role
        self.port = port
        self.peers = [p.rstrip("/") for p in peers if p.strip()]
        self.store = SQLiteStore(db_path)

    @property
    def is_leader(self) -> bool:
        return self.role == "leader"

    def replicate_to_followers(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        failures = []
        for peer in self.peers:
            url = f"{peer}/internal/replicate"
            req = request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with request.urlopen(req, timeout=3) as resp:
                    body = resp.read().decode("utf-8")
                    parsed = json.loads(body) if body else {}
                    if resp.status >= 300 or not parsed.get("ok", False):
                        failures.append(
                            {
                                "peer": peer,
                                "status": resp.status,
                                "response": parsed,
                            }
                        )
            except Exception as exc:
                failures.append({"peer": peer, "error": str(exc)})
        return failures


class Handler(BaseHTTPRequestHandler):
    server_version = "DistSQLiteSQL/0.2"

    @property
    def node(self) -> ReplicatedNode:
        return self.server.node  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{self.node.node_id}] {self.address_string()} - {fmt % args}")

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:
        path = parse.urlparse(self.path).path

        if path == "/health":
            return self._send_json(
                200,
                {
                    "ok": True,
                    "node_id": self.node.node_id,
                    "role": self.node.role,
                    "port": self.node.port,
                    "peers": self.node.peers,
                },
            )

        return self._send_json(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:
        path = parse.urlparse(self.path).path

        if path == "/query":
            return self.handle_query()

        if path == "/execute":
            return self.handle_execute()

        if path == "/internal/replicate":
            return self.handle_replicate()

        return self._send_json(404, {"ok": False, "error": "not found"})

    def handle_query(self) -> None:
        try:
            body = self._read_json()
            sql = body.get("sql")
            params = body.get("params", [])
            if not isinstance(sql, str):
                return self._send_json(400, {"ok": False, "error": "'sql' must be a string"})
            if not isinstance(params, list):
                return self._send_json(400, {"ok": False, "error": "'params' must be a list"})

            result = self.node.store.query(sql, params)
            return self._send_json(
                200,
                {
                    "ok": True,
                    "node_id": self.node.node_id,
                    "role": self.node.role,
                    **result,
                },
            )
        except Exception as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})

    def handle_execute(self) -> None:
        if not self.node.is_leader:
            return self._send_json(403, {"ok": False, "error": "writes must go to the leader"})

        try:
            body = self._read_json()
            statements = normalize_statements(body)

            op_id = str(uuid.uuid4())
            payload = {
                "op_id": op_id,
                "source": self.node.node_id,
                "ts": utc_now(),
                "statements": statements,
            }

            local_result = self.node.store.apply_write_transaction(
                op_id=op_id,
                statements=statements,
                record_op_log=True,
                payload_for_log=payload,
            )

            failures = self.node.replicate_to_followers(payload)

            return self._send_json(
                200,
                {
                    "ok": True,
                    "op_id": op_id,
                    "leader": self.node.node_id,
                    "local_result": local_result,
                    "replication_failures": failures,
                },
            )
        except Exception as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})

    def handle_replicate(self) -> None:
        try:
            body = self._read_json()
            op_id = body.get("op_id")
            statements = body.get("statements")

            if not isinstance(op_id, str) or not op_id:
                return self._send_json(400, {"ok": False, "error": "missing 'op_id'"})
            if not isinstance(statements, list) or not statements:
                return self._send_json(400, {"ok": False, "error": "missing 'statements'"})

            normalized = normalize_statements({"statements": statements})

            result = self.node.store.apply_write_transaction(
                op_id=op_id,
                statements=normalized,
                record_op_log=False,
            )

            return self._send_json(
                200,
                {
                    "ok": True,
                    "node_id": self.node.node_id,
                    "result": result,
                },
            )
        except Exception as exc:
            return self._send_json(400, {"ok": False, "error": str(exc)})


def main() -> None:
    parser = argparse.ArgumentParser(description="Tiny distributed SQLite SQL replicator")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--role", choices=["leader", "follower"], required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument(
        "--peers",
        default="",
        help="Comma-separated follower URLs, e.g. http://127.0.0.1:8002,http://127.0.0.1:8003",
    )
    args = parser.parse_args()

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]

    node = ReplicatedNode(
        node_id=args.node_id,
        role=args.role,
        port=args.port,
        db_path=args.db,
        peers=peers,
    )

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.node = node  # type: ignore[attr-defined]

    print(
        json.dumps(
            {
                "node_id": node.node_id,
                "role": node.role,
                "port": node.port,
                "peers": node.peers,
            },
            indent=2,
        )
    )
    print(f"Serving on http://127.0.0.1:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
