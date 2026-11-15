"""迟到演示：清单晚于事件到达的整笔接入全流程。

依次演示：
  1) 清单迟到：事件先收齐也不发布，位点不动；
  2) 清单到达后事务整笔发布（规范示例 x: None->[g,5]->[h,7]）；
  3) 同内容重放幂等；
  4) 异内容重放报 ValueError；
  5) 不越过未齐事务（事务 3 齐备也压在事务 2 之后）；
  6) 旧像不符：本调用含暂存整体回滚，可用正确内容重收；
  7) 关闭重开续接（明细/汇总/位点均已落库）；
  8) 零和组保留、空组删除；
  9) 非法输入示例。
"""

import os
import tempfile

import warehouse


def show(tag, snap):
    print(f"  [{tag}]")
    print(f"    rows              = {snap['rows']}")
    print(f"    totals            = {snap['totals']}")
    print(f"    published_offsets = {snap['published_offsets']}")


def main():
    tmp = tempfile.TemporaryDirectory(prefix="warehouse-demo-")
    path = os.path.join(tmp.name, "demo.db")
    st = warehouse.Store(path)

    print("== 1) 清单迟到：事务 1 的两个事件先到 ==")
    print("feed('A',0,1,0,'x',None,['g',5])      ->", st.feed("A", 0, 1, 0, "x", None, ["g", 5]))
    print("feed('B',0,1,1,'x',['g',5],['h',7]) ->", st.feed("B", 0, 1, 1, "x", ["g", 5], ["h", 7]))
    show("清单未到，不发布，位点不动", st.snapshot())

    print("== 2) 清单到达，事务 1 整笔发布 ==")
    print("manifest(1,{'A':[0,1],'B':[0,1]}) ->", st.manifest(1, {"A": [0, 1], "B": [0, 1]}))
    show("x: None->[g,5]->[h,7]，仅 h 汇总 7，两位点为 1", st.snapshot())

    print("== 3) 同内容重放：幂等，返回 [] ==")
    print("重放 feed A0   ->", st.feed("A", 0, 1, 0, "x", None, ["g", 5]))
    print("重放 manifest1 ->", st.manifest(1, {"A": [0, 1], "B": [0, 1]}))
    show("状态不变", st.snapshot())

    print("== 4) 异内容重放：ValueError ==")
    try:
        st.feed("A", 0, 1, 0, "x", None, ["g", 6])
    except ValueError as e:
        print("  ValueError:", e)

    print("== 5) 不越过未齐事务 ==")
    print("manifest(2,{'A':[1,2],'B':[1,2]}) ->", st.manifest(2, {"A": [1, 2], "B": [1, 2]}))
    print("feed('B',1,2,1,'x',['h',7],None) ->", st.feed("B", 1, 2, 1, "x", ["h", 7], None))
    print("manifest(3,{'A':[2,2],'B':[2,3]}) ->", st.manifest(3, {"A": [2, 2], "B": [2, 3]}))
    print("feed('B',2,3,0,'z',None,['g',-10]) ->", st.feed("B", 2, 3, 0, "z", None, ["g", -10]))
    show("事务 3 已齐备，但事务 2 缺 A1，一律不发布", st.snapshot())

    print("== 6) 旧像不符：本调用含暂存整体回滚 ==")
    try:
        st.feed("A", 1, 2, 0, "y", ["g", 999], ["g", 10])
    except ValueError as e:
        print("  ValueError:", e)
    print("  错误的 A1 未暂存，同一位点可用正确内容重收：")
    print("feed('A',1,2,0,'y',None,['g',10]) ->", st.feed("A", 1, 2, 0, "y", None, ["g", 10]))
    show("事务 2、3 一次连发；g 组 +10-10=0 仍保留（留零和组）", st.snapshot())

    print("== 7) 关闭重开，状态续接 ==")
    st.close()
    st = warehouse.Store(path)
    show("重开后快照一致", st.snapshot())

    print("== 8) 事务 4 清空 g 组：删空组 ==")
    print("feed('A',2,4,0,'y',['g',10],None)  ->", st.feed("A", 2, 4, 0, "y", ["g", 10], None))
    print("feed('B',3,4,1,'z',['g',-10],None) ->", st.feed("B", 3, 4, 1, "z", ["g", -10], None))
    print("manifest(4,{'A':[2,3],'B':[3,4]}) ->", st.manifest(4, {"A": [2, 3], "B": [3, 4]}))
    show("g 组删空，明细清空", st.snapshot())

    print("== 9) 非法输入示例 ==")
    for label, fn in [
        ("布尔金额", lambda: st.feed("A", 3, 5, 0, "w", None, ["g", True])),
        ("空 id", lambda: st.feed("A", 3, 5, 0, "", None, None)),
        ("位点断档", lambda: st.feed("A", 9, 5, 0, "w", None, None)),
        ("清单断档", lambda: st.manifest(6, {"A": [3, 3], "B": [3, 3]})),
    ]:
        try:
            fn()
            print(f"  {label}: 未报错（不应发生）")
        except ValueError as e:
            print(f"  {label}: ValueError: {e}")

    st.close()
    print("DEMO OK")


if __name__ == "__main__":
    main()
