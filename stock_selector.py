# -*- coding: utf-8 -*-
"""
===================================
概念板块选股器
===================================

职责：
1. 拉取概念板块涨跌排行
2. 选出 Top N 板块
3. 每个板块选出 Top M 只股票，并做去重与补数
"""

from __future__ import annotations

import logging
from typing import List, Dict, Optional

import pandas as pd

from data_provider.akshare_fetcher import AkshareFetcher


logger = logging.getLogger(__name__)


class ConceptBoardSelector:
    """概念板块选股器"""

    def __init__(
        self,
        top_boards: int = 10,
        stocks_per_board: int = 6,
        sleep_min: float = 2.0,
        sleep_max: float = 5.0,
    ) -> None:
        self.top_boards = top_boards
        self.stocks_per_board = stocks_per_board
        self.fetcher = AkshareFetcher(sleep_min=sleep_min, sleep_max=sleep_max)
        self.last_boards: List[Dict[str, Optional[str]]] = []
        self.last_board_summary: Dict[str, List[str]] = {}

    def select(self) -> List[str]:
        """选出概念板块股票列表"""
        boards = self._get_top_concept_boards(self.top_boards)
        self.last_boards = boards
        self.last_board_summary = {}
        if not boards:
            logger.warning("[板块选股] 未获取到概念板块排行，返回空列表")
            return []

        selected_codes: List[str] = []
        selected_set = set()
        board_summary: Dict[str, List[str]] = {}

        for board in boards:
            board_name = board["name"]
            board_code = board.get("code")
            codes = self._select_board_stocks(board_name, selected_set, board_code)
            board_summary[board_name] = codes

            selected_codes.extend(codes)

        logger.info(
            f"[板块选股] 概念板块 Top{len(boards)}，共选出 {len(selected_codes)} 只股票"
        )
        logger.info(
            "[板块选股] 板块明细: "
            + ", ".join(
                f"{name}({len(codes)}只)" for name, codes in board_summary.items()
            )
        )
        self.last_board_summary = board_summary
        return selected_codes

    def _get_top_concept_boards(self, top_n: int) -> List[Dict[str, Optional[str]]]:
        df = self.fetcher.get_concept_board_rankings()
        if df is None or df.empty:
            return []

        name_col = self._pick_column(
            df, ["板块名称", "概念名称", "概念", "名称", "板块", "name"]
        )
        if not name_col:
            logger.warning("[板块选股] 未找到概念板块名称列")
            return []

        code_col = self._pick_column(df, ["板块代码", "概念代码", "代码", "code"])

        change_col = self._pick_column(
            df, ["涨跌幅", "涨跌幅(%)", "涨跌幅%", "涨跌幅(%)"]
        )
        if change_col:
            df = df.copy()
            df[change_col] = pd.to_numeric(df[change_col], errors="coerce")
            df = df.dropna(subset=[change_col])
            df = df.sort_values(by=change_col, ascending=False)

        top_df = df.head(max(top_n, 0))
        boards = []
        for _, row in top_df.iterrows():
            name = str(row[name_col]).strip()
            if not name:
                continue
            code = None
            if code_col:
                code_val = str(row.get(code_col, "")).strip()
                if code_val:
                    code = code_val
            change_pct = None
            if change_col:
                change_pct = row.get(change_col)
                if pd.notna(change_pct):
                    try:
                        change_pct = float(change_pct)
                    except Exception:
                        change_pct = None
            boards.append({"name": name, "code": code, "change_pct": change_pct})

        logger.info(
            f"[板块选股] 概念板块 Top{len(boards)}: {[b['name'] for b in boards]}"
        )
        return boards

    def _select_board_stocks(
        self, board_name: str, selected_set: set, board_code: Optional[str]
    ) -> List[str]:
        df = self.fetcher.get_concept_board_components(board_name, board_code)
        if df is None or df.empty:
            logger.warning(f"[板块选股] {board_name} 成分股为空")
            return []

        df = df.copy()
        rank_col = self._pick_column(
            df,
            [
                "涨跌幅",
                "涨跌幅(%)",
                "涨跌幅%",
                "成交额",
                "成交额(万)",
                "成交额(亿元)",
                "总市值",
            ],
        )
        if rank_col:
            df[rank_col] = pd.to_numeric(df[rank_col], errors="coerce")
            df = df.dropna(subset=[rank_col])
            df = df.sort_values(by=rank_col, ascending=False)

        code_col = self._pick_column(df, ["代码", "股票代码", "证券代码"])
        if not code_col:
            logger.warning(f"[板块选股] {board_name} 未找到股票代码列")
            return []

        selected: List[str] = []
        for raw in df[code_col].tolist():
            code = self._normalize_code(raw)
            if not code:
                continue
            if self._is_excluded_board(code):
                continue
            if code in selected_set:
                continue
            if code in selected:
                continue
            selected.append(code)
            selected_set.add(code)
            if len(selected) >= self.stocks_per_board:
                break

        if len(selected) < self.stocks_per_board:
            logger.warning(
                f"[板块选股] {board_name} 成分股不足 {self.stocks_per_board}，实际 {len(selected)}"
            )

        return selected

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        for col in candidates:
            if col in df.columns:
                return col
        return None

    @staticmethod
    def _is_excluded_board(code: str) -> bool:
        """是否属于科创板或北交所（排除）"""
        if not code or len(code) < 6:
            return False
        if code.startswith("688"):  # 科创板
            return True
        if code.startswith(("43", "83", "87", "88", "92")):  # 北交所
            return True
        return False

    @staticmethod
    def _normalize_code(raw: object) -> Optional[str]:
        if raw is None:
            return None
        code = str(raw).strip()
        if not code:
            return None
        if code.endswith(".0"):
            code = code[:-2]
        if code.isdigit():
            if len(code) < 6:
                code = code.zfill(6)
            if len(code) == 6:
                return code
        return None
