"""独立参照核验：对 warehouse.Store 做定向用例 + 随机差分测试。

RefStore 是与 warehouse.Store 无关的纯内存参照实现（快照时全量重算汇总，
发布时整笔在副本上试算再提交），两者仅共享题目契约。核验内容：
  * 16 组定向用例：规范示例、空事务、零和组/删空组、幂等重放、异内容冲突、
    断档、非法输入、区间首尾相接、事件落区间、q 位置、迟到清单下的归属错误、
    旧像不符整笔回滚、不越过未齐事务、重开续接、关闭后行为、分片位点独立推进。
  * 随机差分：多种子随机操作流（合法事件/清单、幂等重放、异内容、非法输入、
    周期性关闭重开），逐操作比对返回值/ValueError 行为与 snapshot。

退出码 0 表示全部通过，1 表示发现不一致。
"""

import os
import random
import sys
import tempfile

import warehouse


# ==================== 独立参照模型 ====================

def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def _img(v, name):
    if v is None:
        return None
    if isinstance(v, (list, tuple)) and len(v) == 2:
        g, n = v
        if isinstance(g, str) and g and _is_int(n):
            return (g, n)
    raise ValueError(f"{name}: 非法像 {v!r}")


def _ranges(r):
    if not isinstance(r, dict) or set(r.keys()) != {"A", "B"}:
        raise ValueError(f"非法清单 {r!r}")
    out = {}
    for k in ("A", "B"):
        v = r[k]
        if not (isinstance(v, (list, tuple)) and len(v) == 2
                and _is_int(v[0]) and _is_int(v[1]) and 0 <= v[0] <= v[1]):
            raise ValueError(f"非法区间 {v!r}")
        out[k] = (v[0], v[1])
    return out


class RefStore:
    """纯内存参照：与 warehouse.Store 同契约、独立实现。"""

    def __init__(self):
        self.rows = {}        # id -> [grp, amt]（已发布明细）
        self.events = {}      # (s,o) -> (t,q,id,b,a)，b/a 为 None 或 (grp,amt)
        self.next_o = {"A": 0, "B": 0}
        self.mans = {}        # t -> {"A":(l,h), "B":(l,h)}
        self.next_m = 1
        self.next_pub = 1
        self.pub_o = {"A": 0, "B": 0}
        self.closed = False

    def _open(self):
        if self.closed:
            raise ValueError("closed")

    def feed(self, s, o, t, q, id, b, a):
        self._open()
        if not (isinstance(s, str) and s in ("A", "B")):
            raise ValueError(f"非法分片 {s!r}")
        if not (_is_int(o) and o >= 0):
            raise ValueError(f"非法位点 {o!r}")
        if not (_is_int(t) and t >= 1):
            raise ValueError(f"非法事务号 {t!r}")
        if not (_is_int(q) and q >= 0):
            raise ValueError(f"非法序号 {q!r}")
        if not (isinstance(id, str) and id):
            raise ValueError(f"非法 id {id!r}")
        b = _img(b, "b")
        a = _img(a, "a")
        if o > self.next_o[s]:
            raise ValueError("位点断档")
        if o < self.next_o[s]:
            if self.events[(s, o)] != (t, q, id, b, a):
                raise ValueError("异内容重放")
            return []
        m = self.mans.get(t)
        if m is not None:
            l, h = m[s]
            if not (l <= o < h):
                raise ValueError("事件不在本事务区间")
            want = (o - l) if s == "A" else (m["A"][1] - m["A"][0]) + (o - l)
            if q != want:
                raise ValueError("q 位置不符")
        # 暂存；若发布失败，本调用（含暂存）整体回滚
        self.events[(s, o)] = (t, q, id, b, a)
        self.next_o[s] = o + 1
        try:
            return self._publish()
        except ValueError:
            del self.events[(s, o)]
            self.next_o[s] = o
            raise

    def manifest(self, t, r):
        self._open()
        if not (_is_int(t) and t >= 1):
            raise ValueError(f"非法事务号 {t!r}")
        rr = _ranges(r)
        if t > self.next_m:
            raise ValueError("清单断档")
        if t < self.next_m:
            if self.mans[t] != rr:
                raise ValueError("异内容清单")
            return []
        prev = self.mans.get(t - 1)
        for s in ("A", "B"):
            if rr[s][0] != (prev[s][1] if prev else 0):
                raise ValueError("区间未首尾相接")
        self.mans[t] = rr
        self.next_m = t + 1
        try:
            return self._publish()
        except ValueError:
            del self.mans[t]
            self.next_m = t
            raise

    def _publish(self):
        # 整笔在副本上试算，全部成功才提交（等价于落库事务的原子性）
        rows = dict(self.rows)
        pub_o = dict(self.pub_o)
        nxt = self.next_pub
        out = []
        while True:
            m = self.mans.get(nxt)
            if m is None:
                break
            if self.next_o["A"] < m["A"][1] or self.next_o["B"] < m["B"][1]:
                break  # 未齐，不越过
            evs = []
            for s in ("A", "B"):
                l, h = m[s]
                for o in range(l, h):
                    evs.append(self.events[(s, o)])
            for i, e in enumerate(evs):
                if e[0] != nxt:
                    raise ValueError("事件归属其他事务")
                if e[1] != i:
                    raise ValueError("q 序列不符")
            for e in evs:
                cur = rows.get(e[2])
                cur = None if cur is None else (cur[0], cur[1])
                if cur != e[3]:
                    raise ValueError("旧像不符")
                if e[4] is None:
                    rows.pop(e[2], None)
                else:
                    rows[e[2]] = [e[4][0], e[4][1]]
            pub_o["A"], pub_o["B"] = m["A"][1], m["B"][1]
            nxt += 1
            out.append(nxt - 1)
        self.rows, self.pub_o, self.next_pub = rows, pub_o, nxt
        return out

    def snapshot(self):
        self._open()
        rows = [[i, g, n] for i, (g, n) in sorted(self.rows.items())]
        tot = {}
        for g, n in self.rows.values():   # 全量重算，与增量实现互相印证
            tot[g] = tot.get(g, 0) + n
        return {
            "rows": rows,
            "totals": {g: tot[g] for g in sorted(tot)},
            "published_offsets": {"A": self.pub_o["A"], "B": self.pub_o["B"]},
        }

    def close(self):
        self.closed = True


# ==================== 定向用例 ====================

def expect_err(fn, ctx):
    try:
        v = fn()
    except ValueError:
        return
    raise AssertionError(f"{ctx}: 期望 ValueError，实际返回 {v!r}")


def t_spec_example(p):
    st = warehouse.Store(p)
    assert st.feed("A", 0, 1, 0, "x", None, ["g", 5]) == []
    assert st.feed("B", 0, 1, 1, "x", ["g", 5], ["h", 7]) == []
    assert st.manifest(1, {"A": [0, 1], "B": [0, 1]}) == [1]
    snap = st.snapshot()
    assert snap == {"rows": [["x", "h", 7]], "totals": {"h": 7},
                    "published_offsets": {"A": 1, "B": 1}}, snap
    st.close()


def t_empty_snapshot_and_empty_tx(p):
    st = warehouse.Store(p)
    assert st.snapshot() == {"rows": [], "totals": {},
                             "published_offsets": {"A": 0, "B": 0}}
    assert st.manifest(1, {"A": [0, 0], "B": [0, 0]}) == [1]  # 空事务即刻发布
    assert st.snapshot() == {"rows": [], "totals": {},
                             "published_offsets": {"A": 0, "B": 0}}
    st.close()


def t_zero_sum_group_and_delete_empty(p):
    st = warehouse.Store(p)
    st.feed("A", 0, 1, 0, "x", None, ["g", 5])
    st.feed("B", 0, 1, 1, "y", None, ["g", -5])
    assert st.manifest(1, {"A": [0, 1], "B": [0, 1]}) == [1]
    snap = st.snapshot()
    assert snap["totals"] == {"g": 0}, snap          # 留零和组
    assert snap["rows"] == [["x", "g", 5], ["y", "g", -5]], snap
    st.feed("A", 1, 2, 0, "x", ["g", 5], None)
    st.feed("B", 1, 2, 1, "y", ["g", -5], None)
    assert st.manifest(2, {"A": [1, 2], "B": [1, 2]}) == [2]
    snap = st.snapshot()
    assert snap["totals"] == {} and snap["rows"] == [], snap  # 删空组
    st.close()


def t_idempotent_replay(p):
    st = warehouse.Store(p)
    assert st.feed("A", 0, 1, 0, "x", None, ("g", 5)) == []      # 元组像亦可
    assert st.feed("A", 0, 1, 0, "x", None, ["g", 5]) == []      # 同内容重放
    assert st.feed("B", 0, 1, 1, "x", ["g", 5], ["h", 7]) == []
    assert st.manifest(1, {"A": [0, 1], "B": [0, 1]}) == [1]
    before = st.snapshot()
    assert st.manifest(1, {"A": [0, 1], "B": [0, 1]}) == []      # 清单重放
    assert st.feed("A", 0, 1, 0, "x", None, ["g", 5]) == []      # 发布后仍幂等
    assert st.feed("B", 0, 1, 1, "x", ["g", 5], ["h", 7]) == []
    assert st.snapshot() == before
    st.close()


def t_conflicting_replay(p):
    st = warehouse.Store(p)
    st.feed("A", 0, 1, 0, "x", None, ["g", 5])
    assert st.manifest(1, {"A": [0, 1], "B": [0, 0]}) == [1]  # 清单一到即发布
    expect_err(lambda: st.feed("A", 0, 1, 0, "x", None, ["g", 6]), "异内容事件")
    expect_err(lambda: st.feed("A", 0, 2, 0, "x", None, ["g", 5]), "异事务号")
    expect_err(lambda: st.manifest(1, {"A": [0, 2], "B": [0, 0]}), "异内容清单")
    # 冲突不影响既有状态，后续照常
    assert st.feed("B", 0, 2, 0, "y", None, None) == []
    assert st.manifest(2, {"A": [1, 1], "B": [0, 1]}) == [2]
    st.close()


def t_gaps(p):
    st = warehouse.Store(p)
    expect_err(lambda: st.feed("A", 1, 1, 0, "x", None, None), "位点断档")
    expect_err(lambda: st.manifest(2, {"A": [0, 0], "B": [0, 0]}), "清单断档")
    assert st.feed("A", 0, 1, 0, "x", None, None) == []   # 断档报错后仍可正常接续
    assert st.manifest(1, {"A": [0, 1], "B": [0, 0]}) == [1]
    st.close()


def t_bad_inputs(p):
    st = warehouse.Store(p)
    bad_feeds = [
        ("C", 0, 1, 0, "x", None, None),          # 非法分片
        ("A", True, 1, 0, "x", None, None),       # 布尔位点
        ("A", -1, 1, 0, "x", None, None),         # 负位点
        ("A", 0, 0, 0, "x", None, None),          # t=0
        ("A", 0, 1.5, 0, "x", None, None),        # 非整事务号
        ("A", 0, 1, -1, "x", None, None),         # 负 q
        ("A", 0, 1, 0, "", None, None),           # 空 id
        ("A", 0, 1, 0, 7, None, None),            # 非串 id
        ("A", 0, 1, 0, "x", ["g", True], None),   # 布尔金额
        ("A", 0, 1, 0, "x", ["", 1], None),       # 空组名
        ("A", 0, 1, 0, "x", ["g"], None),         # 像长度错
        ("A", 0, 1, 0, "x", ["g", 1, 2], None),
        ("A", 0, 1, 0, "x", "g", None),           # 像非序列
        ("A", 0, 1, 0, "x", None, ["g", 1.5]),    # 非整金额
        ("A", 0, 1, 0, "x", None, [b"g", 1]),     # 组名非串
    ]
    for i, args in enumerate(bad_feeds):
        expect_err(lambda a=args: st.feed(*a), f"非法事件#{i}")
    bad_mans = [
        (True, {"A": [0, 0], "B": [0, 0]}),
        ("1", {"A": [0, 0], "B": [0, 0]}),
        (1, {"A": [0, 0]}),                        # 缺键
        (1, {"A": [0, 0], "B": [0, 0], "C": [0, 0]}),  # 多键
        (1, {"A": [1, 0], "B": [0, 0]}),           # l>h
        (1, {"A": [-1, 0], "B": [0, 0]}),          # 负端点
        (1, {"A": [0, 0.5], "B": [0, 0]}),         # 非整
        (1, {"A": [0, True], "B": [0, 0]}),        # 布尔
        (1, {"A": "x", "B": [0, 0]}),
        (1, None),
    ]
    for i, args in enumerate(bad_mans):
        expect_err(lambda a=args: st.manifest(*a), f"非法清单#{i}")
    # 全部报错后仓库仍正常
    assert st.feed("A", 0, 1, 0, "x", None, ["g", 1]) == []
    assert st.manifest(1, {"A": [0, 1], "B": [0, 0]}) == [1]
    assert st.snapshot()["totals"] == {"g": 1}
    st.close()


def t_interval_continuity(p):
    st = warehouse.Store(p)
    st.manifest(1, {"A": [0, 1], "B": [0, 0]})
    expect_err(lambda: st.manifest(2, {"A": [2, 3], "B": [0, 0]}), "A 未相接")
    expect_err(lambda: st.manifest(2, {"A": [1, 2], "B": [1, 1]}), "B 未相接")
    assert st.manifest(2, {"A": [1, 2], "B": [0, 0]}) == []  # 相接的合法清单
    st.close()


def t_event_out_of_interval(p):
    st = warehouse.Store(p)
    st.manifest(1, {"A": [0, 1], "B": [0, 0]})
    assert st.feed("A", 0, 1, 0, "x", None, ["g", 5]) == [1]
    expect_err(lambda: st.feed("A", 1, 1, 0, "y", None, None), "越出已发布事务区间")
    st.close()


def t_q_position(p):
    st = warehouse.Store(p)
    st.manifest(1, {"A": [0, 1], "B": [0, 1]})
    expect_err(lambda: st.feed("A", 0, 1, 1, "x", None, None), "q 位置错")
    expect_err(lambda: st.feed("B", 0, 1, 0, "y", None, None), "B 的 q 应接在 A 后")
    assert st.feed("A", 0, 1, 0, "x", None, None) == []
    assert st.feed("B", 0, 1, 1, "y", None, None) == [1]
    st.close()


def t_deferred_t_mismatch(p):
    st = warehouse.Store(p)
    assert st.feed("A", 0, 9, 0, "x", None, ["g", 1]) == []  # 清单未到，先暂存
    expect_err(lambda: st.manifest(1, {"A": [0, 1], "B": [0, 0]}), "归属他事务")
    # 出错清单已回滚：可改登不相交的合法空事务 1
    assert st.manifest(1, {"A": [0, 0], "B": [0, 0]}) == [1]
    assert st.snapshot()["published_offsets"] == {"A": 0, "B": 0}
    st.close()


def t_image_mismatch_rollback(p):
    st = warehouse.Store(p)
    st.feed("A", 0, 1, 0, "x", None, ["g", 5])
    st.manifest(1, {"A": [0, 1], "B": [0, 1]})
    before = st.snapshot()
    expect_err(lambda: st.feed("B", 0, 1, 1, "x", ["g", 6], ["h", 7]), "旧像不符")
    assert st.snapshot() == before                     # 本调用整体回滚
    # 错误的 B0 未暂存：同一位点可用正确内容重收并发布
    assert st.feed("B", 0, 1, 1, "x", ["g", 5], ["h", 7]) == [1]
    assert st.snapshot()["totals"] == {"h": 7}
    st.close()


def t_no_skip_incomplete(p):
    st = warehouse.Store(p)
    st.feed("B", 0, 2, 0, "y", None, ["g", 3])               # 事务 2 的唯一事件先到
    assert st.manifest(1, {"A": [0, 1], "B": [0, 0]}) == []  # 事务 1 缺 A0
    assert st.manifest(2, {"A": [1, 1], "B": [0, 1]}) == []  # 事务 2 已齐，但不越过
    assert st.snapshot()["published_offsets"] == {"A": 0, "B": 0}
    assert st.feed("A", 0, 1, 0, "x", None, ["h", 4]) == [1, 2]  # 补齐后连发
    snap = st.snapshot()
    assert snap["rows"] == [["x", "h", 4], ["y", "g", 3]], snap
    assert snap["totals"] == {"g": 3, "h": 4}, snap
    assert snap["published_offsets"] == {"A": 1, "B": 1}, snap
    st.close()


def t_reopen(p):
    st = warehouse.Store(p)
    st.feed("A", 0, 1, 0, "x", None, ["g", 5])
    st.close()
    st = warehouse.Store(p)                       # 重开续接
    assert st.feed("A", 0, 1, 0, "x", None, ["g", 5]) == []  # 重放仍幂等
    st.feed("B", 0, 1, 1, "x", ["g", 5], ["h", 7])
    assert st.manifest(1, {"A": [0, 1], "B": [0, 1]}) == [1]
    snap = st.snapshot()
    st.close()
    st = warehouse.Store(p)
    assert st.snapshot() == snap                   # 明细/汇总/位点均已落库
    st.feed("A", 1, 2, 0, "x", ["h", 7], None)
    assert st.manifest(2, {"A": [1, 2], "B": [1, 1]}) == [2]
    assert st.snapshot() == {"rows": [], "totals": {},
                             "published_offsets": {"A": 2, "B": 1}}
    st.close()


def t_closed_store(p):
    st = warehouse.Store(p)
    st.close()
    st.close()                                     # 重复关闭无害
    expect_err(lambda: st.feed("A", 0, 1, 0, "x", None, None), "关闭后 feed")
    expect_err(lambda: st.manifest(1, {"A": [0, 0], "B": [0, 0]}), "关闭后 manifest")
    expect_err(st.snapshot, "关闭后 snapshot")


def t_per_shard_offsets(p):
    st = warehouse.Store(p)
    st.feed("A", 0, 1, 0, "x", None, ["g", 1])
    st.feed("A", 1, 1, 1, "y", None, ["g", 2])
    assert st.manifest(1, {"A": [0, 2], "B": [0, 0]}) == [1]  # 仅 A 推进
    assert st.snapshot()["published_offsets"] == {"A": 2, "B": 0}
    st.feed("B", 0, 2, 0, "x", ["g", 1], ["h", 9])
    assert st.manifest(2, {"A": [2, 2], "B": [0, 1]}) == [2]  # 仅 B 推进
    snap = st.snapshot()
    assert snap["published_offsets"] == {"A": 2, "B": 1}
    assert snap["rows"] == [["x", "h", 9], ["y", "g", 2]]
    assert snap["totals"] == {"g": 2, "h": 9}
    st.close()


TARGETED = [
    t_spec_example, t_empty_snapshot_and_empty_tx, t_zero_sum_group_and_delete_empty,
    t_idempotent_replay, t_conflicting_replay, t_gaps, t_bad_inputs,
    t_interval_continuity, t_event_out_of_interval, t_q_position,
    t_deferred_t_mismatch, t_image_mismatch_rollback, t_no_skip_incomplete,
    t_reopen, t_closed_store, t_per_shard_offsets,
]


# ==================== 随机差分 ====================

class Mismatch(Exception):
    pass


def apply(store, name, args):
    try:
        return ("ok", getattr(store, name)(*args))
    except ValueError as e:
        return ("err", type(e).__name__)


class Gen:
    """合法操作规划器：维护一份“已规划发布”的像，用来生成自洽的 b/a。"""

    IDS = ("x", "y", "z", "w")
    GROUPS = ("g", "h", "i")

    def __init__(self, rng):
        self.rng = rng
        self.sim = {}                       # id -> [grp, amt]
        self.txs = []
        self.plan_h = {"A": 0, "B": 0}
        self.emitted_o = {"A": 0, "B": 0}
        self.emitted_m = 0
        self.emitted_h = {"A": 0, "B": 0}

    def plan_tx(self):
        t = len(self.txs) + 1
        r, evs = {}, {"A": [], "B": []}
        q = 0
        for s in ("A", "B"):
            l = self.plan_h[s]
            h = l + self.rng.randrange(0, 3)
            r[s] = [l, h]
            for o in range(l, h):
                id_ = self.rng.choice(self.IDS)
                cur = self.sim.get(id_)
                b = None if cur is None else [cur[0], cur[1]]
                if self.rng.random() < 0.25:
                    a = None
                else:
                    a = [self.rng.choice(self.GROUPS), self.rng.randrange(-8, 21)]
                if a is None:
                    self.sim.pop(id_, None)
                else:
                    self.sim[id_] = [a[0], a[1]]
                evs[s].append((o, q, id_, b, a))
                q += 1
            self.plan_h[s] = h
        self.txs.append({"t": t, "r": r, "evs": evs,
                         "m": False, "ptr": {"A": 0, "B": 0}})

    def next_valid(self):
        cands = []
        for s in ("A", "B"):
            for tx in self.txs:             # 分片内按位点序，取最早未发事件
                if tx["ptr"][s] < len(tx["evs"][s]):
                    cands.append(("feed", tx, s))
                    break
        for tx in self.txs:                 # 清单按事务序，取最早未登清单
            if not tx["m"]:
                cands.append(("manifest", tx, None))
                break
        if not cands:
            return None
        kind, tx, s = self.rng.choice(cands)
        if kind == "manifest":
            tx["m"] = True
            self.emitted_m += 1
            self.emitted_h = {"A": tx["r"]["A"][1], "B": tx["r"]["B"][1]}
            return ("manifest",
                    (tx["t"], {"A": list(tx["r"]["A"]), "B": list(tx["r"]["B"])}))
        o, q, id_, b, a = tx["evs"][s][tx["ptr"][s]]
        tx["ptr"][s] += 1
        self.emitted_o[s] += 1

        def conv(v):  # 偶尔以元组形式传像，考验归一化
            if v is None:
                return None
            return tuple(v) if self.rng.random() < 0.3 else list(v)
        return ("feed", (s, o, tx["t"], q, id_, conv(b), conv(a)))


def junk(rng, gen):
    """必为非法的操作（立即报错，不改变状态）。"""
    s = rng.choice(("A", "B"))
    nxt_o = gen.emitted_o[s]
    nxt_m = gen.emitted_m + 1
    opts = [
        ("feed", ("C", 0, 1, 0, "x", None, None)),
        ("feed", (s, True, 1, 0, "x", None, None)),
        ("feed", (s, -1, 1, 0, "x", None, None)),
        ("feed", (s, nxt_o + 1 + rng.randrange(3), 1, 0, "x", None, None)),
        ("feed", (s, nxt_o, 0, 0, "x", None, None)),
        ("feed", (s, nxt_o, 1, -1, "x", None, None)),
        ("feed", (s, nxt_o, 1, 0, "", None, None)),
        ("feed", (s, nxt_o, 1, 0, "x", ["g", True], None)),
        ("feed", (s, nxt_o, 1, 0, "x", ["", 1], None)),
        ("feed", (s, nxt_o, 1, 0, "x", ["g"], None)),
        ("feed", (s, nxt_o, 1, 0, "x", "nope", None)),
        ("feed", (s, nxt_o, 1, 0, "x", None, ["g", 1.5])),
        ("feed", (s, nxt_o, 1, 0, "x", None, ["g", 1, 2])),
        ("manifest", ("1", {"A": [0, 0], "B": [0, 0]})),
        ("manifest", (nxt_m + 1 + rng.randrange(3), {"A": [0, 0], "B": [0, 0]})),
        ("manifest", (max(1, gen.emitted_m), {"A": [0, 1]})),
        ("manifest", (max(1, gen.emitted_m), {"A": [0, 1], "B": [0, 0], "C": [0, 0]})),
        ("manifest", (max(1, gen.emitted_m), {"A": [2, 1], "B": [0, 0]})),
        ("manifest", (max(1, gen.emitted_m), {"A": [-1, 0], "B": [0, 0]})),
        ("manifest", (nxt_m, {"A": [gen.emitted_h["A"] + 2, gen.emitted_h["A"] + 3],
                              "B": [gen.emitted_h["B"], gen.emitted_h["B"]]})),
    ]
    return rng.choice(opts)


def mutate(rng, accepted):
    """已接收操作的异内容版本（保持键不变 -> 必冲突）。"""
    name, args = rng.choice(accepted)
    args = [a for a in args]
    if name == "feed":
        i = rng.choice((2, 3, 4, 5, 6))     # 改 t/q/id/b/a，保持 (s,o)
        if i == 2:
            args[2] = args[2] + 1
        elif i == 3:
            args[3] = args[3] + 1
        elif i == 4:
            args[4] = args[4] + "_x"
        elif i == 5:
            args[5] = None if args[5] is not None else ["zz", 1]
        else:
            args[6] = None if args[6] is not None else ["zz", 1]
    else:
        t, r = args
        r = {"A": list(r["A"]), "B": list(r["B"])}
        r[rng.choice(("A", "B"))][1] += 1
        args = [t, r]
    return (name, tuple(args))


def fuzz(seed, steps, tmp):
    rng = random.Random(seed)
    gen = Gen(rng)
    path = os.path.join(tmp, f"fuzz-{seed}.db")
    real = warehouse.Store(path)
    ref = RefStore()
    accepted = []
    reopens = 0

    def snap_check(ctx):
        a, b = real.snapshot(), ref.snapshot()
        if a != b:
            raise Mismatch(f"[seed={seed}] {ctx}\n  real={a}\n  ref ={b}")

    for i in range(steps):
        r = rng.random()
        expect = None
        if r < 0.06 and accepted:
            real.close()
            real = warehouse.Store(path)    # 重开续接
            reopens += 1
            snap_check(f"第{i}步重开后")
            continue
        if r < 0.16 and accepted:
            op = rng.choice(accepted)       # 同内容重放 -> 幂等
            expect = ("ok", [])
        elif r < 0.30 and accepted:
            op = mutate(rng, accepted)      # 异内容 -> ValueError
            expect = ("err", "ValueError")
        elif r < 0.45:
            op = junk(rng, gen)             # 非法输入 -> ValueError
            expect = ("err", "ValueError")
        else:
            if not gen.txs or rng.random() < 0.35:
                gen.plan_tx()
            op = gen.next_valid()
            if op is None:
                gen.plan_tx()
                op = gen.next_valid()
            expect = "ok"
        name, args = op
        r1, r2 = apply(real, name, args), apply(ref, name, args)
        ctx = f"第{i}步 {name}{args}"
        if r1 != r2:
            raise Mismatch(f"[seed={seed}] {ctx}\n  real={r1}\n  ref ={r2}")
        if expect == "ok":
            if r1[0] != "ok":
                raise Mismatch(f"[seed={seed}] {ctx}: 规划内合法操作未成功: {r1}")
        elif r1 != expect:
            raise Mismatch(f"[seed={seed}] {ctx}: 期望 {expect}，实得 {r1}")
        if r1[0] == "ok":
            accepted.append((name, args))
        snap_check(f"{ctx} -> {r1}")
    real.close()
    return reopens


def main():
    tmp = tempfile.mkdtemp(prefix="warehouse-verify-")
    for fn in TARGETED:
        fn(os.path.join(tmp, fn.__name__ + ".db"))
    print(f"定向用例：{len(TARGETED)} 组全部通过")
    total_reopens = 0
    seeds = (1, 2, 3, 4)
    for seed in seeds:
        total_reopens += fuzz(seed, 800, tmp)
        print(f"随机差分：seed={seed} 800 步通过")
    print(f"VERIFY PASS：{len(TARGETED)} 组定向用例 + "
          f"{len(seeds)} 种子 x 800 步随机差分（含 {total_reopens} 次重开续接）")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AssertionError, Mismatch) as e:
        print(f"VERIFY FAIL: {e}", file=sys.stderr)
        sys.exit(1)
