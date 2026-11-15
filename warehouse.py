"""分片更正整笔接入仓（Python/SQLite，单进程串行）。

    import warehouse
    st = warehouse.Store(path)
    st.feed(s, o, t, q, id, b, a)   # 逐位点接收分片事件 -> [新发布事务号 ...]
    st.manifest(t, r)               # 登记事务 t 的清单   -> [新发布事务号 ...]
    st.snapshot()                   # 当前已发布快照
    st.close()                      # 关闭；重开 Store(path) 可续接

语义约定：
  * s ∈ {'A','B'}；o 为分片位点，各从 0 连续初收；同 (s,o) 同内容重放幂等，
    异内容重放抛 ValueError。
  * t 从 1 连续；manifest(t, r) 中 r={'A':[l,h],'B':[l,h]} 为可空半开区间，
    按 t 各从 0 首尾相接；清单可晚于事件到达。
  * 事件须落在本事务区间内；q 恰为 0..(事务事件数-1)，按 (分片, 位点) 排序。
  * 事务齐备（清单到齐且区间内事件全部收到）且此前事务均已发布时整笔发布：
    依 q 序验旧像 b、写新像 a；像为 None 或 [组, 金额]，None 表缺行；
    id/组为非空字符串，数值均为非布尔整数。
  * 异内容、非法输入或旧像不符抛 ValueError，本调用（含暂存）整体回滚。
  * 接收返回新发布 t 列表，不越过未齐事务；明细/汇总/位点原子落库。
  * 发布仅处理涉事 ID/组（汇总按事件增量维护），不扫全仓、不重放历史。
"""

from __future__ import annotations

import sqlite3

__all__ = ["Store"]

_SHARDS = ("A", "B")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    t   INTEGER PRIMARY KEY,
    a_l INTEGER NOT NULL,
    a_h INTEGER NOT NULL,
    b_l INTEGER NOT NULL,
    b_h INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    s     TEXT NOT NULL,
    o     INTEGER NOT NULL,
    t     INTEGER NOT NULL,
    q     INTEGER NOT NULL,
    id    TEXT NOT NULL,
    b_grp TEXT,
    b_amt INTEGER,
    a_grp TEXT,
    a_amt INTEGER,
    PRIMARY KEY (s, o)
);
CREATE TABLE IF NOT EXISTS rows (
    id  TEXT PRIMARY KEY,
    grp TEXT NOT NULL,
    amt INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS totals (
    grp   TEXT PRIMARY KEY,
    total INTEGER NOT NULL,
    cnt   INTEGER NOT NULL
);
"""

_META_DEFAULTS = {
    "next_o_A": 0,   # 分片 A 下一个待初收位点
    "next_o_B": 0,   # 分片 B 下一个待初收位点
    "next_m_t": 1,   # 下一个待登记清单的事务号
    "next_pub_t": 1, # 下一个待发布的事务号
    "pub_o_A": 0,    # 分片 A 已发布位点（下一发布 o）
    "pub_o_B": 0,    # 分片 B 已发布位点（下一发布 o）
}


def _is_int(x):
    """非布尔整数。"""
    return isinstance(x, int) and not isinstance(x, bool)


def _norm_image(img, name):
    """像归一化：None 或 [组, 金额] -> None 或 (组, 金额)。"""
    if img is None:
        return None
    if isinstance(img, (list, tuple)) and len(img) == 2:
        grp, amt = img
        if isinstance(grp, str) and grp and _is_int(amt):
            return (grp, amt)
    raise ValueError(f"{name}: 像须为 None 或 [非空组名, 非布尔整数金额]，得到 {img!r}")


def _norm_interval(iv, name):
    if (isinstance(iv, (list, tuple)) and len(iv) == 2
            and _is_int(iv[0]) and _is_int(iv[1]) and 0 <= iv[0] <= iv[1]):
        return (iv[0], iv[1])
    raise ValueError(f"{name}: 区间须为整数对 [l, h] 且 0<=l<=h，得到 {iv!r}")


def _norm_ranges(r):
    if not isinstance(r, dict):
        raise ValueError(f"清单须为 {{'A':[l,h],'B':[l,h]}}，得到 {r!r}")
    if set(r.keys()) != {"A", "B"}:
        raise ValueError(f"清单须恰含 'A'、'B' 两键，得到 {sorted(map(repr, r.keys()))}")
    return {"A": _norm_interval(r["A"], "A"), "B": _norm_interval(r["B"], "B")}


class _Atomic:
    """单调用事务：正常结束 COMMIT，任何异常 ROLLBACK（含暂存回滚）。"""

    def __init__(self, db):
        self._db = db

    def __enter__(self):
        self._db.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc, tb):
        self._db.execute("ROLLBACK" if exc_type is not None else "COMMIT")
        return False


class Store:
    def __init__(self, path):
        self._db = sqlite3.connect(str(path), isolation_level=None)
        self._closed = False
        self._db.executescript(_SCHEMA)
        with _Atomic(self._db):
            for k, v in _META_DEFAULTS.items():
                self._db.execute("INSERT OR IGNORE INTO meta(k, v) VALUES (?, ?)", (k, v))

    # ---------------- 内部工具 ----------------

    def _ensure_open(self):
        if self._closed:
            raise ValueError("仓库已关闭")

    def _meta(self, k):
        return self._db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()[0]

    def _set_meta(self, k, v):
        self._db.execute("UPDATE meta SET v=? WHERE k=?", (v, k))

    @staticmethod
    def _img_from_row(grp, amt):
        return None if grp is None else (grp, amt)

    # ---------------- 接收：事件 ----------------

    def feed(self, s, o, t, q, id, b, a):
        self._ensure_open()
        if not (isinstance(s, str) and s in _SHARDS):
            raise ValueError(f"分片 s 须为 'A'/'B'，得到 {s!r}")
        if not (_is_int(o) and o >= 0):
            raise ValueError(f"位点 o 须为 >=0 的非布尔整数，得到 {o!r}")
        if not (_is_int(t) and t >= 1):
            raise ValueError(f"事务号 t 须为 >=1 的非布尔整数，得到 {t!r}")
        if not (_is_int(q) and q >= 0):
            raise ValueError(f"序号 q 须为 >=0 的非布尔整数，得到 {q!r}")
        if not (isinstance(id, str) and id):
            raise ValueError(f"id 须为非空字符串，得到 {id!r}")
        b = _norm_image(b, "b")
        a = _norm_image(a, "a")
        with _Atomic(self._db):
            next_o = self._meta("next_o_" + s)
            if o > next_o:
                raise ValueError(f"位点断档：分片 {s} 期望 o={next_o}，得到 o={o}")
            if o < next_o:
                row = self._db.execute(
                    "SELECT t,q,id,b_grp,b_amt,a_grp,a_amt FROM events WHERE s=? AND o=?",
                    (s, o)).fetchone()
                old = (row[0], row[1], row[2],
                       self._img_from_row(row[3], row[4]),
                       self._img_from_row(row[5], row[6]))
                if old != (t, q, id, b, a):
                    raise ValueError(f"分片 {s} 位点 {o} 已有异内容事件")
                return []  # 同内容重放：幂等
            # 清单已到时，尽早校验“事件须落本事务区间”与 q 的位置
            m = self._db.execute(
                "SELECT a_l,a_h,b_l,b_h FROM manifests WHERE t=?", (t,)).fetchone()
            if m is not None:
                l, h = (m[0], m[1]) if s == "A" else (m[2], m[3])
                if not (l <= o < h):
                    raise ValueError(f"事件 ({s},{o}) 不在事务 {t} 的区间 [{l},{h}) 内")
                want_q = (o - l) if s == "A" else (m[1] - m[0]) + (o - l)
                if q != want_q:
                    raise ValueError(f"事件 ({s},{o}) 的 q 应为 {want_q}，得到 {q}")
            self._db.execute(
                "INSERT INTO events(s,o,t,q,id,b_grp,b_amt,a_grp,a_amt)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (s, o, t, q, id,
                 None if b is None else b[0], None if b is None else b[1],
                 None if a is None else a[0], None if a is None else a[1]))
            self._set_meta("next_o_" + s, o + 1)
            return self._publish_ready()

    # ---------------- 接收：清单 ----------------

    def manifest(self, t, r):
        self._ensure_open()
        if not (_is_int(t) and t >= 1):
            raise ValueError(f"事务号 t 须为 >=1 的非布尔整数，得到 {t!r}")
        rr = _norm_ranges(r)
        with _Atomic(self._db):
            next_m = self._meta("next_m_t")
            if t > next_m:
                raise ValueError(f"清单断档：期望事务 {next_m}，得到 {t}")
            if t < next_m:
                row = self._db.execute(
                    "SELECT a_l,a_h,b_l,b_h FROM manifests WHERE t=?", (t,)).fetchone()
                if (row[0], row[1], row[2], row[3]) != \
                        (rr["A"][0], rr["A"][1], rr["B"][0], rr["B"][1]):
                    raise ValueError(f"事务 {t} 已有异内容清单")
                return []  # 同内容重放：幂等
            prev_a_h = prev_b_h = 0
            if t > 1:
                row = self._db.execute(
                    "SELECT a_h,b_h FROM manifests WHERE t=?", (t - 1,)).fetchone()
                prev_a_h, prev_b_h = row[0], row[1]
            if rr["A"][0] != prev_a_h or rr["B"][0] != prev_b_h:
                raise ValueError(
                    f"事务 {t} 区间未与上一事务首尾相接：期望左端 ({prev_a_h},{prev_b_h})，"
                    f"得到 ({rr['A'][0]},{rr['B'][0]})")
            self._db.execute(
                "INSERT INTO manifests(t,a_l,a_h,b_l,b_h) VALUES(?,?,?,?,?)",
                (t, rr["A"][0], rr["A"][1], rr["B"][0], rr["B"][1]))
            self._set_meta("next_m_t", t + 1)
            return self._publish_ready()

    # ---------------- 整笔发布 ----------------

    def _publish_ready(self):
        """从 next_pub_t 起连续发布齐备事务；遇未齐即停（不越过）。"""
        published = []
        while True:
            t = self._meta("next_pub_t")
            m = self._db.execute(
                "SELECT a_l,a_h,b_l,b_h FROM manifests WHERE t=?", (t,)).fetchone()
            if m is None:
                break
            a_l, a_h, b_l, b_h = m
            if self._meta("next_o_A") < a_h or self._meta("next_o_B") < b_h:
                break  # 事务未齐
            evs = self._db.execute(
                "SELECT s,o,t,q,id,b_grp,b_amt,a_grp,a_amt FROM events"
                " WHERE (s='A' AND o>=? AND o<?) OR (s='B' AND o>=? AND o<?)"
                " ORDER BY s, o", (a_l, a_h, b_l, b_h)).fetchall()
            # 先整体校验：归属本事务、q 恰为 0..n-1
            for i, e in enumerate(evs):
                if e[2] != t:
                    raise ValueError(
                        f"事件 ({e[0]},{e[1]}) 属事务 {e[2]}，不在本事务 {t} 的区间")
                if e[3] != i:
                    raise ValueError(f"事务 {t} 第 {i} 个事件的 q 为 {e[3]}")
            # 再依 q 序验旧像、写新像（仅触及涉事 ID/组）
            for e in evs:
                self._apply(e[4],
                            self._img_from_row(e[5], e[6]),
                            self._img_from_row(e[7], e[8]))
            self._set_meta("pub_o_A", a_h)
            self._set_meta("pub_o_B", b_h)
            self._set_meta("next_pub_t", t + 1)
            published.append(t)
        return published

    def _apply(self, id, b, a):
        row = self._db.execute("SELECT grp,amt FROM rows WHERE id=?", (id,)).fetchone()
        cur = None if row is None else (row[0], row[1])
        if cur != b:
            raise ValueError(f"id {id!r} 旧像不符：事件声明 {b!r}，现为 {cur!r}")
        if b is not None:
            self._totals_add(b[0], -b[1], -1)
        if a is None:
            self._db.execute("DELETE FROM rows WHERE id=?", (id,))
        else:
            cur2 = self._db.execute(
                "UPDATE rows SET grp=?, amt=? WHERE id=?", (a[0], a[1], id))
            if cur2.rowcount == 0:
                self._db.execute(
                    "INSERT INTO rows(id,grp,amt) VALUES(?,?,?)", (id, a[0], a[1]))
            self._totals_add(a[0], a[1], +1)

    def _totals_add(self, grp, d_total, d_cnt):
        """增量维护组汇总；cnt 归零即删空组（零和组保留）。"""
        cur = self._db.execute(
            "UPDATE totals SET total=total+?, cnt=cnt+? WHERE grp=?",
            (d_total, d_cnt, grp))
        if cur.rowcount == 0:
            self._db.execute(
                "INSERT INTO totals(grp,total,cnt) VALUES(?,?,?)", (grp, d_total, d_cnt))
        self._db.execute("DELETE FROM totals WHERE grp=? AND cnt=0", (grp,))

    # ---------------- 快照与关闭 ----------------

    def snapshot(self):
        self._ensure_open()
        rows = [[r[0], r[1], r[2]]
                for r in self._db.execute("SELECT id,grp,amt FROM rows")]
        rows.sort(key=lambda x: x[0])  # 明细按 ID 序
        totals = {g: t for g, t in self._db.execute("SELECT grp,total FROM totals")}
        return {
            "rows": rows,
            "totals": {g: totals[g] for g in sorted(totals)},
            "published_offsets": {"A": self._meta("pub_o_A"),
                                  "B": self._meta("pub_o_B")},
        }

    def close(self):
        if not self._closed:
            self._db.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False
