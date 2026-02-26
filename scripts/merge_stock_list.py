#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
合并板块选股与沙里淘金结果，按打分排序并限制数量

打分规则：
- 板块来源：+1
- 沙里淘金来源：+1，并按 Wyckoff 信号强度（score）加权
- 若同只股票两处都有，分数累加
"""
import json
import os
import sys
from pathlib import Path

# 项目根目录
ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)


def load_concept_codes() -> set:
    """加载板块选股列表"""
    p = Path("concept_stock_list.txt")
    if not p.exists() or not p.stat().st_size:
        return set()
    return set(l.strip() for l in p.read_text(encoding="utf-8").splitlines() if l.strip())


def load_wyckoff_codes_and_signals() -> tuple[set, dict]:
    """加载沙里淘金批次及信号"""
    codes = set()
    signals = {}
    for f in sorted(Path(".").glob("wyckoff_batch_*_signals.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for code, info in data.items():
                codes.add(code)
                if code not in signals or (info.get("score", 0) or 0) > (signals.get(code, {}).get("score", 0) or 0):
                    signals[code] = info
        except Exception:
            pass
    for f in sorted(Path(".").glob("wyckoff_batch_*.txt")):
        if "_signals" in f.name:
            continue
        for l in f.read_text(encoding="utf-8").splitlines():
            c = l.strip()
            if c:
                codes.add(c)
    return codes, signals


def merge_and_rank(
    concept_codes: set,
    wyckoff_codes: set,
    wyckoff_signals: dict,
    max_count: int = 32,
) -> list[str]:
    """
    合并并打分排序，返回 Top N 股票代码列表
    """
    scores = {}
    for code in concept_codes:
        scores[code] = scores.get(code, 0) + 1
    for code in wyckoff_codes:
        base = 1
        extra = (wyckoff_signals.get(code) or {}).get("score") or 0
        if extra > 0:
            base += min(extra / 10, 2)  # 信号强度折算，最多+2
        scores[code] = scores.get(code, 0) + base

    sorted_codes = sorted(scores.keys(), key=lambda c: scores[c], reverse=True)
    return sorted_codes[:max_count]


def main() -> int:
    max_count = int(os.getenv("MAX_STOCK_COUNT", "32"))
    concept = load_concept_codes()
    wyckoff_codes, wyckoff_signals = load_wyckoff_codes_and_signals()

    merged = merge_and_rank(concept, wyckoff_codes, wyckoff_signals, max_count)
    if not merged:
        return 1

    Path("merged_stock_list.txt").write_text("\n".join(merged), encoding="utf-8")
    # 仅保留最终列表中的威科夫信号
    final_signals = {c: wyckoff_signals.get(c, {}) for c in merged if c in wyckoff_signals}
    if final_signals:
        Path("wyckoff_signals.json").write_text(
            json.dumps(final_signals, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(f"合并完成: {len(merged)} 只 (上限 {max_count})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
