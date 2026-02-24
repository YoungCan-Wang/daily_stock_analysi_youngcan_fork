# -*- coding: utf-8 -*-
"""
===================================
大盘复盘分析模块
===================================

职责：
1. 获取大盘指数数据（上证、深证、创业板）
2. 搜索市场新闻形成复盘情报
3. 使用大模型生成每日大盘复盘报告
"""

import logging
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List

import akshare as ak
import pandas as pd

from proxy_utils import no_proxy

from config import get_config
from search_service import SearchService

logger = logging.getLogger(__name__)


@dataclass
class MarketIndex:
    """大盘指数数据"""

    code: str  # 指数代码
    name: str  # 指数名称
    current: float = 0.0  # 当前点位
    change: float = 0.0  # 涨跌点数
    change_pct: float = 0.0  # 涨跌幅(%)
    open: float = 0.0  # 开盘点位
    high: float = 0.0  # 最高点位
    low: float = 0.0  # 最低点位
    prev_close: float = 0.0  # 昨收点位
    volume: float = 0.0  # 成交量（手）
    amount: float = 0.0  # 成交额（元）
    amplitude: float = 0.0  # 振幅(%)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "current": self.current,
            "change": self.change,
            "change_pct": self.change_pct,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "amount": self.amount,
            "amplitude": self.amplitude,
        }


@dataclass
class MarketOverview:
    """市场概览数据"""

    date: str  # 日期
    indices: List[MarketIndex] = field(default_factory=list)  # 主要指数
    up_count: int = 0  # 上涨家数
    down_count: int = 0  # 下跌家数
    flat_count: int = 0  # 平盘家数
    limit_up_count: int = 0  # 涨停家数
    limit_down_count: int = 0  # 跌停家数
    total_amount: float = 0.0  # 两市成交额（亿元）
    north_flow: float = 0.0  # 北向资金净流入（亿元）

    # 板块涨幅榜
    top_sectors: List[Dict] = field(default_factory=list)  # 涨幅前5板块
    bottom_sectors: List[Dict] = field(default_factory=list)  # 跌幅前5板块
    data_note: str = ""  # 数据来源/缓存备注


class MarketAnalyzer:
    """
    大盘复盘分析器

    功能：
    1. 获取大盘指数实时行情
    2. 获取市场涨跌统计
    3. 获取板块涨跌榜
    4. 搜索市场新闻
    5. 生成大盘复盘报告
    """

    # 主要指数代码
    MAIN_INDICES = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000688": "科创50",
        "sh000016": "上证50",
        "sh000300": "沪深300",
    }
    _CACHE_FILE = "data/market_overview_cache.json"

    def __init__(self, search_service: Optional[SearchService] = None, analyzer=None):
        """
        初始化大盘分析器

        Args:
            search_service: 搜索服务实例
            analyzer: AI分析器实例（用于调用LLM）
        """
        self.config = get_config()
        self.search_service = search_service
        self.analyzer = analyzer

    def get_market_overview(self) -> MarketOverview:
        """
        获取市场概览数据

        Returns:
            MarketOverview: 市场概览数据对象
        """
        today = datetime.now().strftime("%Y-%m-%d")
        overview = MarketOverview(date=today)

        # 1. 获取主要指数行情
        overview.indices = self._get_main_indices()

        # 2. 获取涨跌统计
        self._get_market_statistics(overview)

        # 3. 获取板块涨跌榜
        self._get_sector_rankings(overview)

        # 4. 获取北向资金（可选）
        # self._get_north_flow(overview)

        if self._is_overview_empty(overview):
            cached = self._load_cached_overview()
            if cached:
                cached.data_note = f"数据源异常，已回退到最近一次成功缓存（日期：{cached.date}）"
                logger.warning("[大盘] 数据为空，回退到缓存数据")
                return cached
            overview.data_note = "数据源异常，未命中缓存"
            logger.warning("[大盘] 数据为空，且未命中缓存")
            return overview

        self._save_cached_overview(overview)
        return overview

    def get_index_snapshot(self) -> List[MarketIndex]:
        """
        获取主要指数快照（轻量版）

        Returns:
            主要指数列表
        """
        return self._get_main_indices()

    def _call_akshare_with_retry(self, fn, name: str, attempts: int = 2):
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                with self._proxy_disabled():
                    return fn()
            except Exception as e:
                last_error = e
                logger.warning(
                    f"[大盘] {name} 获取失败 (attempt {attempt}/{attempts}): {e}"
                )
                if attempt < attempts:
                    time.sleep(min(2**attempt, 5))
        logger.error(f"[大盘] {name} 最终失败: {last_error}")
        return None

    @contextmanager
    def _proxy_disabled(self):
        if os.getenv("AKSHARE_NO_PROXY", "").strip().lower() not in (
            "1",
            "true",
            "yes",
            "on",
        ):
            yield
            return

        with no_proxy():
            yield

    def _get_main_indices(self) -> List[MarketIndex]:
        """获取主要指数实时行情"""
        indices = []
        results_by_code: Dict[str, MarketIndex] = {}

        try:
            logger.info("[大盘] 获取主要指数实时行情...")

            # 使用 akshare 获取指数行情（新浪财经接口，包含深市指数）
            df = self._call_akshare_with_retry(
                ak.stock_zh_index_spot_sina, "指数行情", attempts=2
            )

            if df is not None and not df.empty:
                for code, name in self.MAIN_INDICES.items():
                    # 查找对应指数
                    row = df[df["代码"] == code]
                    if row.empty:
                        # 尝试带前缀查找
                        row = df[df["代码"].str.contains(code)]

                    if not row.empty:
                        row = row.iloc[0]
                        index = MarketIndex(
                            code=code,
                            name=name,
                            current=float(row.get("最新价", 0) or 0),
                            change=float(row.get("涨跌额", 0) or 0),
                            change_pct=float(row.get("涨跌幅", 0) or 0),
                            open=float(row.get("今开", 0) or 0),
                            high=float(row.get("最高", 0) or 0),
                            low=float(row.get("最低", 0) or 0),
                            prev_close=float(row.get("昨收", 0) or 0),
                            volume=float(row.get("成交量", 0) or 0),
                            amount=float(row.get("成交额", 0) or 0),
                        )
                        # 计算振幅
                        if index.prev_close > 0:
                            index.amplitude = (
                                (index.high - index.low) / index.prev_close * 100
                            )
                        results_by_code[code] = index

                logger.info(f"[大盘] 获取到 {len(results_by_code)} 个指数行情 (AkShare)")

        except Exception as e:
            logger.error(f"[大盘] 获取指数行情失败: {e}")

        # 使用 efinance 补齐缺失指数
        missing = [c for c in self.MAIN_INDICES if c not in results_by_code]
        if missing:
            ef_indices = self._get_main_indices_efinance(missing)
            for idx in ef_indices:
                results_by_code[idx.code] = idx

        indices = [results_by_code[c] for c in self.MAIN_INDICES if c in results_by_code]
        return indices

    def _get_market_statistics(self, overview: MarketOverview):
        """获取市场涨跌统计"""
        try:
            logger.info("[大盘] 获取市场涨跌统计...")

            # 获取全部A股实时行情
            df = self._call_akshare_with_retry(
                ak.stock_zh_a_spot_em, "A股实时行情", attempts=2
            )

            if df is not None and not df.empty:
                applied = self._apply_market_stats_from_df(overview, df)
                if applied:
                    logger.info(
                        f"[大盘] 涨:{overview.up_count} 跌:{overview.down_count} 平:{overview.flat_count} "
                        f"涨停:{overview.limit_up_count} 跌停:{overview.limit_down_count} "
                        f"成交额:{overview.total_amount:.0f}亿 (AkShare)"
                    )
                    return

            # AkShare 失败或字段不完整，回退 efinance
            if self._apply_market_stats_from_efinance(overview):
                logger.info(
                    f"[大盘] 涨:{overview.up_count} 跌:{overview.down_count} 平:{overview.flat_count} "
                    f"涨停:{overview.limit_up_count} 跌停:{overview.limit_down_count} "
                    f"成交额:{overview.total_amount:.0f}亿 (Efinance)"
                )

        except Exception as e:
            logger.error(f"[大盘] 获取涨跌统计失败: {e}")

    def _get_sector_rankings(self, overview: MarketOverview):
        """获取板块涨跌榜"""
        try:
            logger.info("[大盘] 获取板块涨跌榜...")

            # 获取行业板块行情
            df = self._call_akshare_with_retry(
                ak.stock_board_industry_name_em, "行业板块行情", attempts=2
            )

            if df is not None and not df.empty:
                applied = self._apply_sector_rankings_from_df(overview, df)
                if applied:
                    logger.info(
                        f"[大盘] 领涨板块: {[s['name'] for s in overview.top_sectors]} (AkShare)"
                    )
                    logger.info(
                        f"[大盘] 领跌板块: {[s['name'] for s in overview.bottom_sectors]} (AkShare)"
                    )
                    return

            if self._apply_sector_rankings_from_efinance(overview):
                logger.info(
                    f"[大盘] 领涨板块: {[s['name'] for s in overview.top_sectors]} (Efinance)"
                )
                logger.info(
                    f"[大盘] 领跌板块: {[s['name'] for s in overview.bottom_sectors]} (Efinance)"
                )

        except Exception as e:
            logger.error(f"[大盘] 获取板块涨跌榜失败: {e}")

    # def _get_north_flow(self, overview: MarketOverview):
    #     """获取北向资金流入"""
    #     try:
    #         logger.info("[大盘] 获取北向资金...")

    #         # 获取北向资金数据
    #         df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")

    #         if df is not None and not df.empty:
    #             # 取最新一条数据
    #             latest = df.iloc[-1]
    #             if '当日净流入' in df.columns:
    #                 overview.north_flow = float(latest['当日净流入']) / 1e8  # 转为亿元
    #             elif '净流入' in df.columns:
    #                 overview.north_flow = float(latest['净流入']) / 1e8

    #             logger.info(f"[大盘] 北向资金净流入: {overview.north_flow:.2f}亿")

    #     except Exception as e:
    #         logger.warning(f"[大盘] 获取北向资金失败: {e}")

    def search_market_news(self) -> List[Dict]:
        """
        搜索市场新闻

        Returns:
            新闻列表
        """
        if not self.search_service:
            logger.warning("[大盘] 搜索服务未配置，跳过新闻搜索")
            return []

        all_news = []
        today = datetime.now()
        month_str = f"{today.year}年{today.month}月"

        # 多维度搜索
        search_queries = [
            f"A股 大盘 复盘 {month_str}",
            f"股市 行情 分析 今日 {month_str}",
            f"A股 市场 热点 板块 {month_str}",
        ]

        try:
            logger.info("[大盘] 开始搜索市场新闻...")

            for query in search_queries:
                # 使用 search_stock_news 方法，传入"大盘"作为股票名
                response = self.search_service.search_stock_news(
                    stock_code="market",
                    stock_name="大盘",
                    max_results=3,
                    focus_keywords=query.split(),
                )
                if response and response.results:
                    all_news.extend(response.results)
                    logger.info(
                        f"[大盘] 搜索 '{query}' 获取 {len(response.results)} 条结果"
                    )

            logger.info(f"[大盘] 共获取 {len(all_news)} 条市场新闻")

        except Exception as e:
            logger.error(f"[大盘] 搜索市场新闻失败: {e}")

        return all_news

    def generate_market_review(self, overview: MarketOverview, news: List) -> str:
        """
        使用大模型生成大盘复盘报告

        Args:
            overview: 市场概览数据
            news: 市场新闻列表 (SearchResult 对象列表)

        Returns:
            大盘复盘报告文本
        """
        if not self.analyzer or not self.analyzer.is_available():
            logger.warning("[大盘] AI分析器未配置或不可用，使用模板生成报告")
            return self._generate_template_review(overview, news)

        # 构建 Prompt
        prompt = self._build_review_prompt(overview, news)

        try:
            logger.info("[大盘] 调用大模型生成复盘报告...")

            generation_config = {
                "temperature": 0.7,
                "max_output_tokens": 4096,
            }

            # 根据 analyzer 使用的 API 类型调用
            if self.analyzer._use_openai:
                # 使用 OpenAI 兼容 API
                review = self.analyzer._call_openai_api(prompt, generation_config)
            else:
                # 使用 Gemini API
                response = self.analyzer._model.generate_content(
                    prompt,
                    generation_config=generation_config,
                )
                review = response.text.strip() if response and response.text else None

            if review:
                logger.info(f"[大盘] 复盘报告生成成功，长度: {len(review)} 字符")
                return review
            else:
                logger.warning("[大盘] 大模型返回为空")
                return self._generate_template_review(overview, news)

        except Exception as e:
            logger.error(f"[大盘] 大模型生成复盘报告失败: {e}")
            return self._generate_template_review(overview, news)

    def _build_review_prompt(self, overview: MarketOverview, news: List) -> str:
        """构建复盘报告 Prompt"""
        # 指数行情信息（简洁格式，不用emoji）
        indices_text = ""
        for idx in overview.indices:
            direction = (
                "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            )
            indices_text += f"- {idx.name}: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"

        # 板块信息
        top_sectors_text = ", ".join(
            [f"{s['name']}({s['change_pct']:+.2f}%)" for s in overview.top_sectors[:3]]
        )
        bottom_sectors_text = ", ".join(
            [
                f"{s['name']}({s['change_pct']:+.2f}%)"
                for s in overview.bottom_sectors[:3]
            ]
        )

        # 新闻信息 - 支持 SearchResult 对象或字典
        news_text = ""
        for i, n in enumerate(news[:6], 1):
            # 兼容 SearchResult 对象和字典
            if hasattr(n, "title"):
                title = n.title[:50] if n.title else ""
                snippet = n.snippet[:100] if n.snippet else ""
            else:
                title = n.get("title", "")[:50]
                snippet = n.get("snippet", "")[:100]
            news_text += f"{i}. {title}\n   {snippet}\n"

        data_note_text = (
            f"数据备注: {overview.data_note}\n\n" if overview.data_note else ""
        )

        prompt = f"""你是一位专业的A股市场分析师，请根据以下数据生成一份简洁的大盘复盘报告。

【重要】输出要求：
- 必须输出纯 Markdown 文本格式
- 禁止输出 JSON 格式
- 禁止输出代码块
- emoji 仅在标题处少量使用（每个标题最多1个）

---

# 今日市场数据

## 日期
{overview.date}

## 主要指数
{indices_text}

## 市场概况
- 上涨: {overview.up_count} 家 | 下跌: {overview.down_count} 家 | 平盘: {overview.flat_count} 家
- 涨停: {overview.limit_up_count} 家 | 跌停: {overview.limit_down_count} 家
- 两市成交额: {overview.total_amount:.0f} 亿元
- 北向资金: {overview.north_flow:+.2f} 亿元
{data_note_text}

## 板块表现
领涨: {top_sectors_text}
领跌: {bottom_sectors_text}

## 市场新闻
{news_text if news_text else "暂无相关新闻"}

---

# 输出格式模板（请严格按此格式输出）

## 📊 {overview.date} 大盘复盘

### 一、市场总结
（2-3句话概括今日市场整体表现，必须引用至少2个具体数值，如指数涨跌%、成交额、涨跌家数）

### 二、指数点评
（分析上证、深证、创业板等各指数走势特点，必须包含具体点位和涨跌幅）

### 三、资金动向
（解读成交额和北向资金流向的含义，必须包含“成交额X亿元、北向资金X亿元”）

### 四、热点解读
（分析领涨领跌板块背后的逻辑和驱动因素）

### 五、后市展望
（结合当前走势和新闻，给出明日市场预判）

### 六、风险提示
（需要关注的风险点）

---

请直接输出复盘报告内容，不要输出其他说明文字。
"""
        return prompt

    def _generate_template_review(self, overview: MarketOverview, news: List) -> str:
        """使用模板生成复盘报告（无大模型时的备选方案）"""

        # 判断市场走势
        sh_index = next((idx for idx in overview.indices if idx.code == "000001"), None)
        if sh_index:
            if sh_index.change_pct > 1:
                market_mood = "强势上涨"
            elif sh_index.change_pct > 0:
                market_mood = "小幅上涨"
            elif sh_index.change_pct > -1:
                market_mood = "小幅下跌"
            else:
                market_mood = "明显下跌"
        else:
            market_mood = "震荡整理"

        # 指数行情（简洁格式）
        indices_text = ""
        for idx in overview.indices[:4]:
            direction = (
                "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            )
            indices_text += f"- **{idx.name}**: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"

        # 板块信息
        top_text = "、".join([s["name"] for s in overview.top_sectors[:3]])
        bottom_text = "、".join([s["name"] for s in overview.bottom_sectors[:3]])

        note_line = f"*数据备注: {overview.data_note}*\n\n" if overview.data_note else ""
        report = f"""## 📊 {overview.date} 大盘复盘

{note_line}

### 一、市场总结
今日A股市场整体呈现**{market_mood}**态势。

### 二、主要指数
{indices_text}

### 三、涨跌统计
| 指标 | 数值 |
|------|------|
| 上涨家数 | {overview.up_count} |
| 下跌家数 | {overview.down_count} |
| 涨停 | {overview.limit_up_count} |
| 跌停 | {overview.limit_down_count} |
| 两市成交额 | {overview.total_amount:.0f}亿 |
| 北向资金 | {overview.north_flow:+.2f}亿 |

### 四、板块表现
- **领涨**: {top_text}
- **领跌**: {bottom_text}

### 五、风险提示
市场有风险，投资需谨慎。以上数据仅供参考，不构成投资建议。

---
*复盘时间: {datetime.now().strftime("%H:%M")}*
"""
        return report

    def _pick_column(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        for col in candidates:
            if col in df.columns:
                return col
        return None

    def _normalize_index_code(self, raw: object) -> str:
        if raw is None:
            return ""
        s = str(raw).strip()
        if not s:
            return ""
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) >= 6:
            return digits[-6:]
        return digits

    def _get_main_indices_efinance(self, missing_codes: List[str]) -> List[MarketIndex]:
        try:
            import efinance as ef
        except Exception as exc:
            logger.warning(f"[大盘] efinance 不可用，跳过指数回退: {exc}")
            return []

        try:
            logger.info("[大盘] 尝试使用 efinance 获取指数行情...")
            df = ef.stock.get_realtime_quotes("沪深系列指数")
        except Exception as exc:
            logger.warning(f"[大盘] efinance 指数行情获取失败: {exc}")
            return []

        if df is None or df.empty:
            return []

        code_col = self._pick_column(df, ["代码", "code", "股票代码"])
        name_col = self._pick_column(df, ["名称", "name", "股票名称"])
        price_col = self._pick_column(df, ["最新价", "最新", "最新价(元)", "最新价/元"])
        change_col = self._pick_column(df, ["涨跌额", "涨跌", "涨跌额(元)"])
        pct_col = self._pick_column(df, ["涨跌幅", "涨跌幅(%)", "涨跌幅%"])
        open_col = self._pick_column(df, ["今开", "开盘", "开盘价"])
        high_col = self._pick_column(df, ["最高", "最高价"])
        low_col = self._pick_column(df, ["最低", "最低价"])
        prev_col = self._pick_column(df, ["昨收", "昨收价", "前收盘价"])
        vol_col = self._pick_column(df, ["成交量", "成交量(手)"])
        amt_col = self._pick_column(df, ["成交额", "成交额(元)"])

        if not code_col:
            return []

        missing_base = {self._normalize_index_code(c) for c in missing_codes}
        indices: List[MarketIndex] = []

        for _, row in df.iterrows():
            code_raw = row.get(code_col)
            code_base = self._normalize_index_code(code_raw)
            if code_base not in missing_base:
                continue

            full_code = next((c for c in missing_codes if c.endswith(code_base)), code_base)
            name = self.MAIN_INDICES.get(full_code, str(row.get(name_col, "")).strip())

            def num(val):
                try:
                    return float(val)
                except Exception:
                    return 0.0

            index = MarketIndex(
                code=full_code,
                name=name,
                current=num(row.get(price_col)) if price_col else 0.0,
                change=num(row.get(change_col)) if change_col else 0.0,
                change_pct=num(row.get(pct_col)) if pct_col else 0.0,
                open=num(row.get(open_col)) if open_col else 0.0,
                high=num(row.get(high_col)) if high_col else 0.0,
                low=num(row.get(low_col)) if low_col else 0.0,
                prev_close=num(row.get(prev_col)) if prev_col else 0.0,
                volume=num(row.get(vol_col)) if vol_col else 0.0,
                amount=num(row.get(amt_col)) if amt_col else 0.0,
            )
            if index.prev_close > 0:
                index.amplitude = (index.high - index.low) / index.prev_close * 100
            indices.append(index)

        return indices

    def _apply_market_stats_from_df(self, overview: MarketOverview, df: pd.DataFrame) -> bool:
        change_col = self._pick_column(df, ["涨跌幅", "涨跌幅(%)", "涨跌幅%"])
        amount_col = self._pick_column(df, ["成交额", "成交额(元)", "成交额(亿)", "成交额(万)"])

        if not change_col and not amount_col:
            return False

        if change_col and change_col in df.columns:
            df[change_col] = pd.to_numeric(df[change_col], errors="coerce")
            overview.up_count = len(df[df[change_col] > 0])
            overview.down_count = len(df[df[change_col] < 0])
            overview.flat_count = len(df[df[change_col] == 0])
            overview.limit_up_count = len(df[df[change_col] >= 9.9])
            overview.limit_down_count = len(df[df[change_col] <= -9.9])

        if amount_col and amount_col in df.columns:
            df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
            total = df[amount_col].sum()
            if "亿" in amount_col:
                overview.total_amount = total
            elif "万" in amount_col:
                overview.total_amount = total / 1e4
            else:
                overview.total_amount = total / 1e8

        return True

    def _apply_market_stats_from_efinance(self, overview: MarketOverview) -> bool:
        try:
            import efinance as ef
        except Exception as exc:
            logger.warning(f"[大盘] efinance 不可用，跳过涨跌统计回退: {exc}")
            return False

        try:
            df = ef.stock.get_realtime_quotes("沪深A股")
        except Exception as exc:
            logger.warning(f"[大盘] efinance A股行情获取失败: {exc}")
            return False

        if df is None or df.empty:
            return False

        return self._apply_market_stats_from_df(overview, df)

    def _apply_sector_rankings_from_df(self, overview: MarketOverview, df: pd.DataFrame) -> bool:
        name_col = self._pick_column(df, ["板块名称", "名称", "板块", "name"])
        change_col = self._pick_column(df, ["涨跌幅", "涨跌幅(%)", "涨跌幅%"])
        if not name_col or not change_col:
            return False

        df = df.copy()
        df[change_col] = pd.to_numeric(df[change_col], errors="coerce")
        df = df.dropna(subset=[change_col])
        if df.empty:
            return False

        top = df.nlargest(5, change_col)
        overview.top_sectors = [
            {"name": row[name_col], "change_pct": row[change_col]}
            for _, row in top.iterrows()
        ]

        bottom = df.nsmallest(5, change_col)
        overview.bottom_sectors = [
            {"name": row[name_col], "change_pct": row[change_col]}
            for _, row in bottom.iterrows()
        ]
        return True

    def _apply_sector_rankings_from_efinance(self, overview: MarketOverview) -> bool:
        try:
            import efinance as ef
        except Exception as exc:
            logger.warning(f"[大盘] efinance 不可用，跳过板块回退: {exc}")
            return False

        try:
            df = ef.stock.get_realtime_quotes("行业板块")
        except Exception as exc:
            logger.warning(f"[大盘] efinance 行业板块获取失败: {exc}")
            return False

        if df is None or df.empty:
            return False

        return self._apply_sector_rankings_from_df(overview, df)

    def _is_overview_empty(self, overview: MarketOverview) -> bool:
        indices_ok = len(overview.indices) > 0
        stats_ok = any(
            [
                overview.up_count,
                overview.down_count,
                overview.flat_count,
                overview.limit_up_count,
                overview.limit_down_count,
                overview.total_amount,
            ]
        )
        sectors_ok = bool(overview.top_sectors or overview.bottom_sectors)
        return not (indices_ok or stats_ok or sectors_ok)

    def _save_cached_overview(self, overview: MarketOverview) -> None:
        try:
            data = {
                "date": overview.date,
                "indices": [idx.to_dict() for idx in overview.indices],
                "up_count": overview.up_count,
                "down_count": overview.down_count,
                "flat_count": overview.flat_count,
                "limit_up_count": overview.limit_up_count,
                "limit_down_count": overview.limit_down_count,
                "total_amount": overview.total_amount,
                "north_flow": overview.north_flow,
                "top_sectors": overview.top_sectors,
                "bottom_sectors": overview.bottom_sectors,
            }
            os.makedirs(os.path.dirname(self._CACHE_FILE), exist_ok=True)
            with open(self._CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as exc:
            logger.debug(f"[大盘] 缓存写入失败: {exc}")

    def _load_cached_overview(self) -> Optional[MarketOverview]:
        try:
            if not os.path.exists(self._CACHE_FILE):
                return None
            with open(self._CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            overview = MarketOverview(date=data.get("date", datetime.now().strftime("%Y-%m-%d")))
            overview.indices = [
                MarketIndex(
                    code=item.get("code", ""),
                    name=item.get("name", ""),
                    current=float(item.get("current", 0) or 0),
                    change=float(item.get("change", 0) or 0),
                    change_pct=float(item.get("change_pct", 0) or 0),
                    open=float(item.get("open", 0) or 0),
                    high=float(item.get("high", 0) or 0),
                    low=float(item.get("low", 0) or 0),
                    prev_close=float(item.get("prev_close", 0) or 0),
                    volume=float(item.get("volume", 0) or 0),
                    amount=float(item.get("amount", 0) or 0),
                    amplitude=float(item.get("amplitude", 0) or 0),
                )
                for item in data.get("indices", [])
            ]
            overview.up_count = int(data.get("up_count", 0) or 0)
            overview.down_count = int(data.get("down_count", 0) or 0)
            overview.flat_count = int(data.get("flat_count", 0) or 0)
            overview.limit_up_count = int(data.get("limit_up_count", 0) or 0)
            overview.limit_down_count = int(data.get("limit_down_count", 0) or 0)
            overview.total_amount = float(data.get("total_amount", 0) or 0)
            overview.north_flow = float(data.get("north_flow", 0) or 0)
            overview.top_sectors = data.get("top_sectors", []) or []
            overview.bottom_sectors = data.get("bottom_sectors", []) or []
            return overview
        except Exception as exc:
            logger.debug(f"[大盘] 缓存读取失败: {exc}")
            return None

    def run_daily_review(self) -> str:
        """
        执行每日大盘复盘流程

        Returns:
            复盘报告文本
        """
        logger.info("========== 开始大盘复盘分析 ==========")

        # 1. 获取市场概览
        overview = self.get_market_overview()

        # 2. 搜索市场新闻
        news = self.search_market_news()

        # 3. 生成复盘报告
        report = self.generate_market_review(overview, news)

        logger.info("========== 大盘复盘分析完成 ==========")

        return report


# 测试入口
if __name__ == "__main__":
    import sys

    sys.path.insert(0, ".")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    )

    analyzer = MarketAnalyzer()

    # 测试获取市场概览
    overview = analyzer.get_market_overview()
    print(f"\n=== 市场概览 ===")
    print(f"日期: {overview.date}")
    print(f"指数数量: {len(overview.indices)}")
    for idx in overview.indices:
        print(f"  {idx.name}: {idx.current:.2f} ({idx.change_pct:+.2f}%)")
    print(f"上涨: {overview.up_count} | 下跌: {overview.down_count}")
    print(f"成交额: {overview.total_amount:.0f}亿")

    # 测试生成模板报告
    report = analyzer._generate_template_review(overview, [])
    print(f"\n=== 复盘报告 ===")
    print(report)
