"""
SQLite database layer using FTS5 for full-text search.
"""
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


CREATE_FILES_TABLE = """
CREATE TABLE IF NOT EXISTS files (
    item_id     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    path        TEXT NOT NULL,
    extension   TEXT,
    size_bytes  INTEGER DEFAULT 0,
    modified_at TEXT,
    content_text TEXT,
    indexed_at  TEXT DEFAULT (datetime('now'))
);
"""

CREATE_FTS_TABLE = """
CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
    item_id UNINDEXED,
    name,
    path UNINDEXED,
    content_text,
    content='files',
    content_rowid='rowid'
);
"""

CREATE_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
  INSERT INTO files_fts(rowid, item_id, name, content_text)
    VALUES (new.rowid, new.item_id, new.name, new.content_text);
END;

CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
  INSERT INTO files_fts(files_fts, rowid, item_id, name, content_text)
    VALUES ('delete', old.rowid, old.item_id, old.name, old.content_text);
END;

CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
  INSERT INTO files_fts(files_fts, rowid, item_id, name, content_text)
    VALUES ('delete', old.rowid, old.item_id, old.name, old.content_text);
  INSERT INTO files_fts(rowid, item_id, name, content_text)
    VALUES (new.rowid, new.item_id, new.name, new.content_text);
END;
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def init(self):
        with self._conn() as conn:
            conn.execute(CREATE_FILES_TABLE)
            conn.execute(CREATE_FTS_TABLE)
            for stmt in CREATE_TRIGGERS.strip().split(";"):
                s = stmt.strip()
                if s:
                    try:
                        conn.execute(s)
                    except sqlite3.OperationalError:
                        pass  # Trigger already exists

    def upsert_file(self, item_id, name, path, extension, size_bytes, modified_at, content_text):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO files (item_id, name, path, extension, size_bytes, modified_at, content_text, indexed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(item_id) DO UPDATE SET
                    name=excluded.name,
                    path=excluded.path,
                    extension=excluded.extension,
                    size_bytes=excluded.size_bytes,
                    modified_at=excluded.modified_at,
                    content_text=excluded.content_text,
                    indexed_at=excluded.indexed_at
            """, (item_id, name, path, extension, size_bytes, modified_at, content_text))

    def delete_item(self, item_id: str):
        with self._conn() as conn:
            conn.execute("DELETE FROM files WHERE item_id = ?", (item_id,))

    def search(self, query: str, mode: str = "both", extensions: list = None, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            # Build FTS query (escape special chars)
            fts_query = query.replace('"', '""')

            if mode == "name":
                fts_filter = f'name: "{fts_query}"'
            elif mode == "content":
                fts_filter = f'content_text: "{fts_query}"'
            else:
                fts_filter = f'"{fts_query}"'

            ext_clause = ""
            params = [fts_filter, limit]
            if extensions:
                placeholders = ",".join("?" * len(extensions))
                ext_clause = f"AND f.extension IN ({placeholders})"
                params = [fts_filter] + extensions + [limit]

            sql = f"""
                SELECT
                    f.item_id, f.name, f.path, f.extension,
                    f.size_bytes, f.modified_at,
                    snippet(files_fts, 3, '[[', ']]', '...', 32) AS content_snippet,
                    CASE
                        WHEN lower(f.name) LIKE lower('%'||?||'%') THEN 'name'
                        ELSE 'content'
                    END AS match_type,
                    rank
                FROM files_fts
                JOIN files f ON f.rowid = files_fts.rowid
                WHERE files_fts MATCH ?
                {ext_clause}
                ORDER BY rank
                LIMIT ?
            """
            # Adjust params for the LIKE check
            all_params = [query, fts_filter] + (extensions or []) + [limit]

            try:
                rows = conn.execute(sql, all_params).fetchall()
                return [dict(r) for r in rows]
            except sqlite3.OperationalError:
                # Fallback: LIKE search
                return self._fallback_search(conn, query, mode, extensions, limit)

    def _fallback_search(self, conn, query: str, mode: str, extensions: list, limit: int) -> list[dict]:
        """LIKE-based search fallback if FTS fails."""
        q = f"%{query}%"
        clauses = []
        params = []

        if mode in ("name", "both"):
            clauses.append("name LIKE ?")
            params.append(q)
        if mode in ("content", "both"):
            clauses.append("content_text LIKE ?")
            params.append(q)

        where = f"({' OR '.join(clauses)})" if clauses else "1=1"
        ext_clause = ""
        if extensions:
            placeholders = ",".join("?" * len(extensions))
            ext_clause = f"AND extension IN ({placeholders})"
            params.extend(extensions)

        params.append(limit)
        sql = f"""
            SELECT item_id, name, path, extension, size_bytes, modified_at,
                   content_text AS content_snippet,
                   CASE WHEN lower(name) LIKE lower(?) THEN 'name' ELSE 'content' END AS match_type
            FROM files
            WHERE {where} {ext_clause}
            LIMIT ?
        """
        params_with_like = [q] + params
        rows = conn.execute(sql, params_with_like).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> dict:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            with_content = conn.execute(
                "SELECT COUNT(*) FROM files WHERE content_text IS NOT NULL AND content_text != ''"
            ).fetchone()[0]
            total_size = conn.execute("SELECT SUM(size_bytes) FROM files").fetchone()[0] or 0
            last_indexed = conn.execute(
                "SELECT MAX(indexed_at) FROM files"
            ).fetchone()[0]

            ext_rows = conn.execute(
                "SELECT extension, COUNT(*) as cnt FROM files WHERE extension IS NOT NULL "
                "GROUP BY extension ORDER BY cnt DESC LIMIT 20"
            ).fetchall()
            by_extension = {r["extension"]: r["cnt"] for r in ext_rows}

            return {
                "total_files": total,
                "with_content": with_content,
                "total_size_mb": total_size / (1024 * 1024),
                "last_indexed": last_indexed,
                "by_extension": by_extension,
            }
