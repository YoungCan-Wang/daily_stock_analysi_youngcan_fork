# -*- coding: utf-8 -*-
"""
===================================
沙里淘金 - 命令行版
===================================

功能：
1. 按指定战术筛选股票
2. 输出 Markdown 报告
3. 通过通知模块推送（飞书/企微/Telegram 等）
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import akshare as ak
from dotenv import load_dotenv

from notification import NotificationService


logger = logging.getLogger(__name__)


@dataclass
class ResisterConfig:
    benchmark_code: str = "000001"
    lookback_window: int = 3
    benchmark_drop_threshold: float = -2.0
    relative_strength_threshold: float = 2.0


@dataclass
class JumperConfig:
    consolidation_window: int = 60
    box_range: float = 0.25
    squeeze_window: int = 5
    squeeze_amplitude: float = 0.05
    volume_dry_ratio: float = 0.6
    volume_long_window: int = 50


@dataclass
class AnomalyConfig:
    volume_spike_ratio: float = 2.5
    stall_pct_limit: float = 2.0
    panic_pct_floor: float = -3.0
    volume_window: int = 5


@dataclass
class FirstBoardConfig:
    exclude_st: bool = True
    exclude_new_days: int = 30
    min_market_cap: float = 200000.0
    max_market_cap: float = 10000000.0
    lookback_limit_days: int = 10
    breakout_window: int = 60


@dataclass
class ScreenerConfig:
    trading_days: int = 500
    resister: ResisterConfig = field(default_factory=ResisterConfig)
    jumper: JumperConfig = field(default_factory=JumperConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    first_board: FirstBoardConfig = field(default_factory=FirstBoardConfig)


_CACHE_DIR = Path(__file__).resolve().parent / "data" / "wyckoff_cache"
_TRADE_DATES_CACHE = (
    Path(__file__).resolve().parent / "data" / "wyckoff_trade_dates.json"
)
_STOCK_LIST_CACHE = Path(__file__).resolve().parent / "data" / "wyckoff_stock_list.json"


def _ensure_cache_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_key(prefix: str, symbol: str, start: str, end: str, adjust: str) -> str:
    safe = f"{prefix}_{symbol}_{start}_{end}_{adjust}".replace("/", "_")
    return safe


def _cache_path(key: str) -> Path:
    _ensure_cache_dir()
    return _CACHE_DIR / f"{key}.csv"


def _load_cache(key: str) -> Optional[pd.DataFrame]:
    path = _cache_path(key)
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _save_cache(key: str, df: pd.DataFrame) -> None:
    path = _cache_path(key)
    try:
        df.to_csv(path, index=False)
    except Exception:
        return


def _trade_dates() -> List[date]:
    cache_ttl = 7 * 24 * 60 * 60
    try:
        if _TRADE_DATES_CACHE.exists():
            age = time.time() - _TRADE_DATES_CACHE.stat().st_mtime
            if age <= cache_ttl:
                with _TRADE_DATES_CACHE.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
                dates = [pd.to_datetime(x).date() for x in raw]
                dates.sort()
                if dates:
                    return dates
    except Exception:
        pass

    dates: List[date] = []
    try:
        df = ak.tool_trade_date_hist_sina()
        if df is not None and not df.empty:
            col = None
            for candidate in ("trade_date", "日期", "trade_date"):
                if candidate in df.columns:
                    col = candidate
                    break
            if col:
                dates = pd.to_datetime(df[col]).dt.date.tolist()
    except Exception:
        dates = []

    if not dates:
        start = date(1990, 1, 1)
        end = date.today() + timedelta(days=366)
        dates = pd.bdate_range(start=start, end=end).date.tolist()

    dates.sort()
    try:
        _TRADE_DATES_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with _TRADE_DATES_CACHE.open("w", encoding="utf-8") as f:
            json.dump([d.strftime("%Y-%m-%d") for d in dates], f, ensure_ascii=False)
    except Exception:
        pass
    return dates


def _resolve_trading_window(
    end_calendar_day: date, trading_days: int
) -> Tuple[date, date]:
    if trading_days <= 0:
        raise ValueError("trading_days must be > 0")
    dates = _trade_dates()
    if not dates:
        raise RuntimeError("trade calendar empty")
    idx = max(0, len(dates) - 1)
    while idx >= 0 and dates[idx] > end_calendar_day:
        idx -= 1
    if idx < 0:
        raise RuntimeError("trade calendar has no date <= end_calendar_day")
    if idx - (trading_days - 1) < 0:
        raise RuntimeError("trade calendar does not have enough historical dates")
    start_trade = dates[idx - (trading_days - 1)]
    end_trade = dates[idx]
    return start_trade, end_trade


def _normalize_hist(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "涨跌幅": "pct_chg",
            "pctChg": "pct_chg",
        }
    )
    keep = ["date", "open", "close", "high", "low", "volume", "pct_chg"]
    out = df[[c for c in keep if c in df.columns]].copy()
    for col in ["open", "close", "high", "low", "volume", "pct_chg"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _normalize_hist_baostock(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "date": "date",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "volume": "volume",
            "pctChg": "pct_chg",
        }
    )
    keep = ["date", "open", "close", "high", "low", "volume", "pct_chg"]
    out = df[[c for c in keep if c in df.columns]].copy()
    for col in ["open", "close", "high", "low", "volume", "pct_chg"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _fetch_hist(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start,
        end_date=end,
        adjust=adjust,
    )
    if df is None or df.empty:
        raise RuntimeError(
            f"empty data returned for symbol={symbol}, start={start}, end={end}, adjust={adjust!r}"
        )
    return df


def _fetch_hist_baostock(symbol: str, start: str, end: str) -> pd.DataFrame:
    import baostock as bs

    def normalize_code(code: str) -> str:
        if code.startswith("sh.") or code.startswith("sz."):
            return code
        if code.startswith(("600", "601", "603", "605", "688")):
            return f"sh.{code}"
        return f"sz.{code}"

    bs_code = normalize_code(symbol)
    start_date = datetime.strptime(start, "%Y%m%d").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y%m%d").strftime("%Y-%m-%d")

    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock login failed: {login.error_msg}")
    try:
        rs = bs.query_history_k_data_plus(
            code=bs_code,
            fields="date,open,high,low,close,volume,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",
        )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock query failed: {rs.error_msg}")
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        if not data_list:
            raise RuntimeError(f"baostock empty data for {symbol}")
        return pd.DataFrame(data_list, columns=rs.fields)
    finally:
        bs.logout()


def _fetch_index_hist(code: str, start: str, end: str) -> pd.DataFrame:
    df = ak.index_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end)
    if df is None or df.empty:
        raise RuntimeError(f"empty index data: {code}")
    return df


def _fetch_index_hist_baostock(code: str, start: str, end: str) -> pd.DataFrame:
    import baostock as bs

    def normalize_index(idx: str) -> str:
        if idx.startswith("sh.") or idx.startswith("sz."):
            return idx
        if idx.startswith("000"):
            return f"sh.{idx}"
        return f"sz.{idx}"

    bs_code = normalize_index(code)
    start_date = datetime.strptime(start, "%Y%m%d").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y%m%d").strftime("%Y-%m-%d")

    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock login failed: {login.error_msg}")
    try:
        rs = bs.query_history_k_data_plus(
            code=bs_code,
            fields="date,open,high,low,close,volume,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",
        )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock query failed: {rs.error_msg}")
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        if not data_list:
            raise RuntimeError(f"baostock empty data for index {code}")
        return pd.DataFrame(data_list, columns=rs.fields)
    finally:
        bs.logout()


def _load_hist_with_source(
    symbol: str,
    start: str,
    end: str,
    adjust: str,
    use_cache: bool,
) -> Tuple[pd.DataFrame, str]:
    cache_key = _cache_key("stock", symbol, start, end, adjust or "none")
    if use_cache:
        cached = _load_cache(cache_key)
        if cached is not None and not cached.empty:
            return cached, "cache"
    try:
        df = _fetch_hist(symbol=symbol, start=start, end=end, adjust=adjust)
        df = _normalize_hist(df)
        if use_cache:
            _save_cache(cache_key, df)
        return df, "akshare"
    except Exception:
        df = _fetch_hist_baostock(symbol=symbol, start=start, end=end)
        df = _normalize_hist_baostock(df)
        if use_cache:
            _save_cache(cache_key, df)
        return df, "baostock"


def _fetch_index_hist_with_source(
    code: str, start: str, end: str, use_cache: bool
) -> Tuple[pd.DataFrame, str]:
    cache_key = _cache_key("index", code, start, end, "none")
    if use_cache:
        cached = _load_cache(cache_key)
        if cached is not None and not cached.empty:
            return cached, "cache"
    try:
        df = _fetch_index_hist(code, start, end)
        df = _normalize_hist(df)
        if use_cache:
            _save_cache(cache_key, df)
        return df, "akshare"
    except Exception:
        df = _fetch_index_hist_baostock(code, start, end)
        df = _normalize_hist_baostock(df)
        if use_cache:
            _save_cache(cache_key, df)
        return df, "baostock"


_sector_cache: Dict[str, str] = {}
_list_date_cache: Dict[str, Optional[date]] = {}
_name_map_cache: Dict[str, str] = {}


def _stock_sector(symbol: str) -> str:
    if symbol in _sector_cache:
        return _sector_cache[symbol]
    try:
        df = ak.stock_individual_info_em(symbol=symbol)
        if df is None or df.empty:
            _sector_cache[symbol] = ""
            return ""
        row = df.loc[df["item"] == "行业", "value"]
        if row.empty:
            _sector_cache[symbol] = ""
            return ""
        value = str(row.iloc[0]).strip()
        _sector_cache[symbol] = value
        return value
    except Exception:
        _sector_cache[symbol] = ""
        return ""


def _stock_list_date(symbol: str) -> Optional[date]:
    if symbol in _list_date_cache:
        return _list_date_cache[symbol]
    try:
        df = ak.stock_individual_info_em(symbol=symbol)
        if df is None or df.empty:
            _list_date_cache[symbol] = None
            return None
        row = df.loc[df["item"] == "上市日期", "value"]
        if row.empty:
            _list_date_cache[symbol] = None
            return None
        raw = str(row.iloc[0]).strip()
        value = datetime.strptime(raw, "%Y-%m-%d").date()
        _list_date_cache[symbol] = value
        return value
    except Exception:
        _list_date_cache[symbol] = None
        return None


def _estimate_market_cap(symbol: str) -> float:
    try:
        df = ak.stock_individual_info_em(symbol=symbol)
        if df is None or df.empty:
            return 0.0
        row = df.loc[df["item"] == "总市值", "value"]
        if row.empty:
            return 0.0
        raw = str(row.iloc[0]).strip()
        if raw.endswith("亿"):
            return float(raw.replace("亿", "")) * 10000
        if raw.endswith("万"):
            return float(raw.replace("万", ""))
        return float(raw)
    except Exception:
        return 0.0


def _parse_manual_symbols(text: str) -> List[str]:
    if not text:
        return []
    candidates = re.findall(r"\d{6}", text)
    out: List[str] = []
    seen = set()
    for code in candidates:
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _load_stock_list() -> List[Dict[str, str]]:
    cache_ttl = 24 * 60 * 60
    try:
        if _STOCK_LIST_CACHE.exists():
            age = time.time() - _STOCK_LIST_CACHE.stat().st_mtime
            if age <= cache_ttl:
                with _STOCK_LIST_CACHE.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, list):
                    return [
                        {"code": str(x.get("code", "")), "name": str(x.get("name", ""))}
                        for x in raw
                        if isinstance(x, dict)
                    ]
    except Exception:
        pass

    try:
        info = ak.stock_info_a_code_name()
        info["code"] = info["code"].astype(str)
        info["name"] = info["name"].astype(str)
        records = info.to_dict("records")
        _STOCK_LIST_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with _STOCK_LIST_CACHE.open("w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
        return records
    except Exception:
        return []


def _stock_name_map() -> Dict[str, str]:
    if _name_map_cache:
        return _name_map_cache
    items = _load_stock_list()
    _name_map_cache.update(
        {x.get("code", ""): x.get("name", "") for x in items if isinstance(x, dict)}
    )
    return _name_map_cache


def _parse_board_list(board_name: str) -> List[str]:
    if not board_name:
        return ["all"]
    raw = board_name.strip().lower()
    if raw in {"all", "main", "chinext", "star", "bse"}:
        return [raw]
    for sep in [",", "+", "|", ";"]:
        if sep in raw:
            parts = [p.strip() for p in raw.split(sep) if p.strip()]
            return parts or [raw]
    return [raw]


def _match_board(code: str, board_name: str) -> bool:
    if board_name == "all":
        return True
    if board_name == "star":
        return code.startswith("688")
    if board_name == "chinext":
        return code.startswith(("300", "301"))
    if board_name == "bse":
        return code.startswith(("43", "83", "87", "88", "92"))
    if board_name == "main":
        return code.startswith(
            (
                "600",
                "601",
                "603",
                "605",
                "000",
                "001",
                "002",
                "003",
            )
        )
    return False


def get_stocks_by_board(board_name: str) -> List[Dict[str, str]]:
    all_stocks = _load_stock_list()
    boards = _parse_board_list(board_name)
    if "all" in boards:
        return all_stocks
    out: List[Dict[str, str]] = []
    seen = set()
    for s in all_stocks:
        code = s.get("code", "")
        if not code:
            continue
        if code in seen:
            continue
        if any(_match_board(code, b) for b in boards):
            out.append(s)
            seen.add(code)
    return out


def _calc_cumulative_pct(df: pd.DataFrame) -> float:
    changes = df["pct_chg"].dropna() / 100.0
    return float((changes + 1).prod() - 1)


def screen_resisters(
    data_map: Dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame,
    cfg: ResisterConfig,
) -> List[Tuple[str, float]]:
    if benchmark_df is None or benchmark_df.empty:
        return []
    bench = benchmark_df.sort_values("date").tail(cfg.lookback_window)
    if len(bench) < cfg.lookback_window:
        return []
    bench_cum = _calc_cumulative_pct(bench)
    if bench_cum * 100 >= cfg.benchmark_drop_threshold:
        return []
    results: List[Tuple[str, float]] = []
    for symbol, df in data_map.items():
        window = df.sort_values("date").tail(cfg.lookback_window)
        if len(window) < cfg.lookback_window:
            continue
        stock_cum = _calc_cumulative_pct(window)
        score = (stock_cum - bench_cum) * 100
        if stock_cum >= 0 or score >= cfg.relative_strength_threshold:
            results.append((symbol, score))
    return results


def screen_anomalies(
    data_map: Dict[str, pd.DataFrame], cfg: AnomalyConfig
) -> List[Tuple[str, float]]:
    results: List[Tuple[str, float]] = []
    for symbol, df in data_map.items():
        df = df.sort_values("date")
        if len(df) < cfg.volume_window + 5:
            continue
        recent = df.iloc[-1]
        if recent["high"] <= recent["low"]:
            continue
        body = abs(recent["close"] - recent["open"])
        upper = recent["high"] - max(recent["open"], recent["close"])
        lower = min(recent["open"], recent["close"]) - recent["low"]
        vol_ma = df["volume"].rolling(window=cfg.volume_window).mean().iloc[-1]
        if vol_ma <= 0:
            continue
        vol_ratio = recent["volume"] / vol_ma
        pct_chg = float(recent["pct_chg"])

        high_stall = (
            vol_ratio >= cfg.volume_spike_ratio
            and pct_chg < cfg.stall_pct_limit
            and (upper >= 2 * body or recent["close"] < recent["open"])
        )
        low_support = (
            vol_ratio >= cfg.volume_spike_ratio
            and pct_chg > cfg.panic_pct_floor
            and lower >= 2 * body
        )
        if high_stall or low_support:
            score = float(vol_ratio)
            results.append((symbol, score))
    return results


def screen_jumpers(
    data_map: Dict[str, pd.DataFrame],
    cfg: JumperConfig,
) -> Tuple[List[Tuple[str, float]], Dict[str, int]]:
    results: List[Tuple[str, float]] = []
    stats = {
        "total": 0,
        "box_pass": 0,
        "squeeze_pass": 0,
        "volume_pass": 0,
        "position_pass": 0,
    }
    window = max(cfg.consolidation_window, 20)
    for symbol, df in data_map.items():
        stats["total"] += 1
        df = df.sort_values("date")
        if len(df) < window:
            continue
        recent = df.iloc[-window:]
        high = recent["high"].max()
        low = recent["low"].min()
        last_close = recent.iloc[-1]["close"]
        if last_close <= 0:
            continue
        box_range = (high - low) / last_close
        if box_range > cfg.box_range:
            continue
        stats["box_pass"] += 1

        short = recent.tail(cfg.squeeze_window)
        short_high = short["high"].max()
        short_low = short["low"].min()
        short_close = short.iloc[-1]["close"]
        if short_close <= 0:
            continue
        short_amp = (short_high - short_low) / short_close
        if short_amp > cfg.squeeze_amplitude:
            continue
        stats["squeeze_pass"] += 1

        vol_short = short["volume"].mean()
        vol_long = recent["volume"].tail(cfg.volume_long_window).mean()
        if vol_long <= 0:
            continue
        if vol_short >= vol_long * cfg.volume_dry_ratio:
            continue
        stats["volume_pass"] += 1

        near_top = last_close >= low + (high - low) * 0.8
        near_bottom = last_close <= low + (high - low) * 0.2
        if near_top or near_bottom:
            stats["position_pass"] += 1
            score = float(short_amp * 100)
            results.append((symbol, score))
    return results, stats


def screen_first_board(
    data_map: Dict[str, pd.DataFrame],
    cfg: FirstBoardConfig,
) -> List[Tuple[str, float]]:
    results: List[Tuple[str, float]] = []
    for symbol, df in data_map.items():
        df = df.sort_values("date")
        if len(df) < 2:
            continue
        if cfg.exclude_st:
            name = _stock_name_map().get(symbol, "")
            if "ST" in name.upper():
                continue
        if cfg.exclude_new_days > 0:
            list_date = _stock_list_date(symbol)
            if list_date is not None:
                if (date.today() - list_date).days < cfg.exclude_new_days:
                    continue
        last = df.iloc[-1]
        prev = df.iloc[-2]
        limit = 20.0 if symbol.startswith(("300", "301", "688")) else 10.0
        threshold = limit * 0.98

        curr_pct = float(last["pct_chg"])
        last_prev_pct = float(prev["pct_chg"])

        is_limit_up = curr_pct >= threshold
        prev_limit = last_prev_pct >= threshold
        if not is_limit_up or prev_limit:
            continue

        recent_limits = df.tail(cfg.lookback_limit_days).copy()
        recent_limits["pct_chg"] = pd.to_numeric(
            recent_limits["pct_chg"], errors="coerce"
        )
        if (recent_limits["pct_chg"] >= threshold).sum() > 1:
            continue
        breakout_window = df.tail(cfg.breakout_window)
        if last["close"] < breakout_window["close"].max():
            continue
        if abs(last["high"] - last["low"]) < 1e-6:
            continue
        if cfg.min_market_cap > 0 or cfg.max_market_cap > 0:
            cap = _estimate_market_cap(symbol)
            if cap:
                if cfg.min_market_cap > 0 and cap < cfg.min_market_cap:
                    continue
                if cfg.max_market_cap > 0 and cap > cfg.max_market_cap:
                    continue
        score = float(last["pct_chg"])
        results.append((symbol, score))
    return results


def _build_sector_summary(
    results: Dict[str, List[Tuple[str, float]]],
) -> Tuple[str, Dict[str, int]]:
    sector_counts: Dict[str, int] = {}
    for pairs in results.values():
        for code, _ in pairs:
            sector = _stock_sector(code)
            if not sector:
                continue
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
    if not sector_counts:
        return "", {}
    top = sorted(sector_counts.items(), key=lambda x: (-x[1], x[0]))[:8]
    summary = "，".join([f"{name}({count})" for name, count in top])
    return summary, sector_counts


def _sort_by_sector_power(
    pairs: List[Tuple[str, float]], sector_counts: Dict[str, int]
) -> List[Tuple[str, float]]:
    return sorted(
        pairs,
        key=lambda item: (-sector_counts.get(_stock_sector(item[0]), 0), item[0]),
    )


def _render_results(
    results: Dict[str, List[Tuple[str, float]]],
    label_map: Dict[str, str],
    score_map: Dict[str, str],
    sector_counts: Optional[Dict[str, int]],
    result_limit: int,
) -> List[str]:
    lines: List[str] = []
    name_map = _stock_name_map()
    for key, label in label_map.items():
        if key not in results:
            continue
        pairs = results.get(key, [])
        lines.append(f"## {label}")
        if not pairs:
            lines.append("无")
            lines.append("")
            continue
        if sector_counts:
            pairs = _sort_by_sector_power(pairs, sector_counts)
        if result_limit > 0:
            pairs = pairs[:result_limit]
        score_label = score_map.get(key, "评分")
        for code, score in pairs:
            name = name_map.get(code, "")
            name_part = f" {name}" if name else ""
            lines.append(f"- {code}{name_part} | {score_label}: {score:.2f}")
        lines.append("")
    return lines


def _load_env_config() -> Dict[str, object]:
    def _get_bool(key: str, default: bool = False) -> bool:
        raw = os.getenv(key)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "y"}

    def _get_int(key: str, default: int) -> int:
        raw = os.getenv(key)
        try:
            return int(raw) if raw is not None else default
        except Exception:
            return default

    def _get_float(key: str, default: float) -> float:
        raw = os.getenv(key)
        try:
            return float(raw) if raw is not None else default
        except Exception:
            return default

    def _get_str(key: str, default: str) -> str:
        raw = os.getenv(key)
        return str(raw).strip() if raw is not None else default

    cfg = {
        "enabled": _get_bool("WYCKOFF_ENABLED", False),
        "tactic": _get_str("WYCKOFF_TACTIC", "all"),
        "trading_days": _get_int("WYCKOFF_TRADING_DAYS", 500),
        "pool_mode": _get_str("WYCKOFF_POOL_MODE", "board"),
        "symbols": _get_str("WYCKOFF_SYMBOLS", ""),
        "board": _get_str("WYCKOFF_BOARD", "all"),
        "limit_count": _get_int("WYCKOFF_LIMIT_COUNT", 500),
        "offset": _get_int("WYCKOFF_OFFSET", 0),
        "group_power": _get_bool("WYCKOFF_GROUP_POWER", True),
        "use_cache": _get_bool("WYCKOFF_USE_CACHE", True),
        "debug": _get_bool("WYCKOFF_DEBUG", False),
        "result_limit": _get_int("WYCKOFF_RESULT_LIMIT", 80),
        "resister": ResisterConfig(
            benchmark_code=_get_str("WYCKOFF_BENCHMARK_CODE", "000001"),
            lookback_window=_get_int("WYCKOFF_LOOKBACK_WINDOW", 3),
            benchmark_drop_threshold=_get_float("WYCKOFF_BENCH_DROP", -2.0),
            relative_strength_threshold=_get_float("WYCKOFF_RS_THRESHOLD", 2.0),
        ),
        "jumper": JumperConfig(
            consolidation_window=_get_int("WYCKOFF_CONSOLIDATION_WINDOW", 60),
            box_range=_get_float("WYCKOFF_BOX_RANGE", 0.25),
            squeeze_window=_get_int("WYCKOFF_SQUEEZE_WINDOW", 5),
            squeeze_amplitude=_get_float("WYCKOFF_SQUEEZE_AMPLITUDE", 0.05),
            volume_dry_ratio=_get_float("WYCKOFF_VOLUME_DRY", 0.6),
            volume_long_window=_get_int("WYCKOFF_VOLUME_LONG", 50),
        ),
        "anomaly": AnomalyConfig(
            volume_spike_ratio=_get_float("WYCKOFF_VOL_SPIKE", 2.5),
            stall_pct_limit=_get_float("WYCKOFF_STALL_LIMIT", 2.0),
            panic_pct_floor=_get_float("WYCKOFF_PANIC_FLOOR", -3.0),
            volume_window=_get_int("WYCKOFF_VOLUME_WINDOW", 5),
        ),
        "first_board": FirstBoardConfig(
            exclude_st=_get_bool("WYCKOFF_EXCLUDE_ST", True),
            exclude_new_days=_get_int("WYCKOFF_EXCLUDE_NEW_DAYS", 30),
            min_market_cap=_get_float("WYCKOFF_MIN_MARKET_CAP", 200000.0),
            max_market_cap=_get_float("WYCKOFF_MAX_MARKET_CAP", 10000000.0),
            lookback_limit_days=_get_int("WYCKOFF_LOOKBACK_LIMIT", 10),
            breakout_window=_get_int("WYCKOFF_BREAKOUT_WINDOW", 60),
        ),
    }
    return cfg


def _resolve_tactic(raw: str) -> str:
    text = (raw or "").strip().lower()
    mapping = {
        "抗跌主力": "resisters",
        "突破临界": "jumpers",
        "异常吸筹/出货": "anomalies",
        "启动龙头": "first_board",
        "resister": "resisters",
        "resisters": "resisters",
        "jumper": "jumpers",
        "jumpers": "jumpers",
        "anomaly": "anomalies",
        "anomalies": "anomalies",
        "first": "first_board",
        "first_board": "first_board",
        "all": "all",
    }
    return mapping.get(text, "all")


def _select_symbols(
    pool_mode: str,
    symbols_text: str,
    board: str,
    limit_count: int,
    offset: int,
) -> List[str]:
    if pool_mode == "manual":
        return _parse_manual_symbols(symbols_text)
    stocks = get_stocks_by_board(board)
    codes = [s.get("code") for s in stocks if s.get("code")]
    if offset > 0:
        codes = codes[offset:]
    if limit_count > 0:
        return codes[:limit_count]
    return codes


def _build_report(
    results: Dict[str, List[Tuple[str, float]]],
    errors: Dict[str, str],
    source_map: Dict[str, str],
    benchmark_source: str,
    cfg: ScreenerConfig,
    meta: Dict[str, object],
    jump_stats: Optional[Dict[str, int]],
) -> str:
    label_map = {
        "resisters": "抗跌主力（相对强弱）",
        "anomalies": "异常吸筹/出货（量价背离）",
        "jumpers": "突破临界（箱体挤压）",
        "first_board": "启动龙头（首板）",
    }
    score_map = {
        "resisters": "RS(%)",
        "anomalies": "量比",
        "jumpers": "短期振幅(%)",
        "first_board": "涨幅(%)",
    }

    sector_summary = ""
    sector_counts = None
    if meta.get("group_power"):
        sector_summary, sector_counts = _build_sector_summary(results)

    cache_hits = sum(1 for source in source_map.values() if source == "cache")
    data_count = len(source_map)
    total_count = int(meta.get("symbols_count", 0))

    lines: List[str] = [
        "# 🧭 沙里淘金",
        f"> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> 战术：{meta.get('tactic_label', '全部')} | 交易日：{cfg.trading_days} | 数据成功：{data_count}/{total_count}",
    ]
    if meta.get("pool_mode") == "manual":
        lines.append(f"> 池：手动输入 | 数量：{total_count}")
    else:
        offset = int(meta.get("offset", 0))
        lines.append(
            f"> 池：板块 {meta.get('board', 'all')} | 数量：{total_count} | 上限：{meta.get('limit_count', 0)} | 起始偏移：{offset}"
        )
    if source_map:
        lines.append(f"> 数据来源：缓存 {cache_hits} | 实时 {data_count - cache_hits}")
    if benchmark_source:
        lines.append(f"> 基准指数来源：{benchmark_source}")
    lines.append("")

    if sector_summary:
        lines.append(f"- 板块共振：{sector_summary}")
        lines.append("")

    lines.extend(
        _render_results(
            results,
            label_map=label_map,
            score_map=score_map,
            sector_counts=sector_counts,
            result_limit=int(meta.get("result_limit", 0)),
        )
    )

    if jump_stats:
        lines.append("## 筛选过程")
        lines.append(
            f"- 总数 {jump_stats.get('total', 0)} | 箱体 {jump_stats.get('box_pass', 0)} | 挤压 {jump_stats.get('squeeze_pass', 0)} | 缩量 {jump_stats.get('volume_pass', 0)} | 位置 {jump_stats.get('position_pass', 0)}"
        )
        lines.append("")

    if errors:
        lines.append("## 失败明细")
        if meta.get("debug"):
            for code, msg in list(errors.items())[:30]:
                lines.append(f"- {code}: {msg}")
        else:
            lines.append(f"- 失败数量：{len(errors)}")
        lines.append("")

    return "\n".join(lines).strip()


def run_panning(
    config: Dict[str, object], tactic_override: Optional[str] = None
) -> str:
    cfg = ScreenerConfig(
        trading_days=int(config["trading_days"]),
        resister=config["resister"],
        jumper=config["jumper"],
        anomaly=config["anomaly"],
        first_board=config["first_board"],
    )

    pool_mode = str(config["pool_mode"]).strip().lower()
    symbols_text = str(config["symbols"]).strip()
    board = str(config["board"]).strip().lower()
    limit_count = int(config["limit_count"])
    offset = int(config.get("offset", 0))
    use_cache = bool(config["use_cache"])
    group_power = bool(config["group_power"])
    debug = bool(config["debug"])
    result_limit = int(config["result_limit"])

    tactic_key = _resolve_tactic(tactic_override or str(config["tactic"]))

    symbols = _select_symbols(pool_mode, symbols_text, board, limit_count, offset)
    symbols = [s for s in symbols if s]
    if not symbols:
        raise RuntimeError(
            "未获取到股票池，请检查 WYCKOFF_POOL_MODE/WYCKOFF_SYMBOLS 配置"
        )

    start_trade, end_trade = _resolve_trading_window(
        end_calendar_day=date.today() - timedelta(days=1),
        trading_days=int(cfg.trading_days),
    )
    start = start_trade.strftime("%Y%m%d")
    end = end_trade.strftime("%Y%m%d")

    data_map: Dict[str, pd.DataFrame] = {}
    source_map: Dict[str, str] = {}
    errors: Dict[str, str] = {}

    for symbol in symbols:
        try:
            df, source = _load_hist_with_source(
                symbol, start, end, adjust="qfq", use_cache=use_cache
            )
            data_map[symbol] = df
            source_map[symbol] = source
        except Exception as exc:
            errors[symbol] = str(exc)

    benchmark_df = None
    benchmark_source = ""
    results: Dict[str, List[Tuple[str, float]]] = {}
    jump_stats = None

    if tactic_key in ("resisters", "all"):
        try:
            benchmark_df, benchmark_source = _fetch_index_hist_with_source(
                cfg.resister.benchmark_code, start, end, use_cache=use_cache
            )
        except Exception as exc:
            errors[cfg.resister.benchmark_code] = f"benchmark failed: {exc}"
        results["resisters"] = screen_resisters(data_map, benchmark_df, cfg.resister)

    if tactic_key in ("jumpers", "all"):
        jumpers, jump_stats = screen_jumpers(data_map, cfg.jumper)
        results["jumpers"] = jumpers

    if tactic_key in ("anomalies", "all"):
        results["anomalies"] = screen_anomalies(data_map, cfg.anomaly)

    if tactic_key in ("first_board", "all"):
        results["first_board"] = screen_first_board(data_map, cfg.first_board)

    # 可选：将筛选出的股票代码写入文件（供 workflow 作为 STOCK_LIST 使用）
    output_list_path = os.getenv("WYCKOFF_OUTPUT_LIST", "").strip()
    if output_list_path:
        all_symbols = set()
        for tactic_results in results.values():
            for symbol, _ in tactic_results:
                all_symbols.add(symbol)
        if all_symbols:
            Path(output_list_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_list_path).write_text(
                "\n".join(sorted(all_symbols)), encoding="utf-8"
            )
            logger.info(f"已写入 {len(all_symbols)} 只股票代码到 {output_list_path}")

    tactic_label_map = {
        "all": "全部",
        "resisters": "抗跌主力",
        "jumpers": "突破临界",
        "anomalies": "异常吸筹/出货",
        "first_board": "启动龙头",
    }

    report = _build_report(
        results=results,
        errors=errors,
        source_map=source_map,
        benchmark_source=benchmark_source,
        cfg=cfg,
        meta={
            "symbols_count": len(symbols),
            "tactic_label": tactic_label_map.get(tactic_key, tactic_key),
            "pool_mode": pool_mode,
            "board": board,
            "limit_count": limit_count,
            "offset": offset,
            "group_power": group_power,
            "debug": debug,
            "result_limit": result_limit,
        },
        jump_stats=jump_stats if tactic_key in ("jumpers", "all") else None,
    )
    return report


def _setup_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )


def main() -> int:
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")

    parser = argparse.ArgumentParser(description="沙里淘金 - 命令行版")
    parser.add_argument("--tactic", type=str, default="", help="覆盖战术")
    parser.add_argument("--no-notify", action="store_true", help="不发送通知")
    args = parser.parse_args()

    _setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    config = _load_env_config()
    if not config.get("enabled"):
        logger.info("WYCKOFF_ENABLED 未启用，跳过执行")
        return 0

    try:
        report = run_panning(config, tactic_override=args.tactic or None)
    except Exception as exc:
        logger.error(f"沙里淘金执行失败: {exc}")
        return 1

    notifier = NotificationService()
    offset = int(config.get("offset", 0))
    limit_count = int(config.get("limit_count", 0))
    batch_tag = f"o{offset}_n{limit_count}" if limit_count > 0 else f"o{offset}_all"
    report_file = notifier.save_report_to_file(
        report,
        filename=f"wyckoff_panning_{datetime.now().strftime('%Y%m%d')}_{batch_tag}.md",
    )
    logger.info(f"沙里淘金报告已保存: {report_file}")

    if not args.no_notify:
        if notifier.is_available():
            notifier.send(report)
        else:
            logger.info("未配置通知渠道，跳过推送")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
