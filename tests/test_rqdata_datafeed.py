import importlib
import sys
import types
from datetime import datetime
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import pytest
from numpy import ndarray

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest


class _RqState:
    def __init__(self) -> None:
        self.price_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.init_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.frame: pd.DataFrame | None = None

    def init(self, *args: object, **kwargs: object) -> None:
        self.init_calls.append((args, kwargs))
        raise RuntimeError("rqdatac.init must not run")

    def get_price(self, *args: object, **kwargs: object) -> pd.DataFrame | None:
        self.price_calls.append((args, kwargs))
        return self.frame

    def get_dominant_price(self, *args: object, **kwargs: object) -> pd.DataFrame | None:
        raise RuntimeError("rqdatac.get_dominant_price must not run")

    def all_instruments(self, *args: object, **kwargs: object) -> pd.DataFrame:
        raise RuntimeError("rqdatac.all_instruments must not run")

    def get_next_trading_date(self, date: datetime) -> datetime:
        return date


def _purge(prefix: str) -> None:
    for name in list(sys.modules):
        if name == prefix or name.startswith(prefix + "."):
            del sys.modules[name]


def _vendor_module(name: str) -> types.ModuleType:
    module: types.ModuleType = types.ModuleType(name)
    module.__path__ = []
    module.__package__ = name
    sys.modules[name] = module
    if "." in name:
        parent, child = name.rsplit(".", 1)
        setattr(sys.modules[parent], child, module)
    return module


def _install_rqdatac(state: _RqState) -> None:
    _purge("rqdatac")
    rqdatac: types.ModuleType = _vendor_module("rqdatac")
    _vendor_module("rqdatac.services")
    get_price_mod: types.ModuleType = _vendor_module("rqdatac.services.get_price")
    future_mod: types.ModuleType = _vendor_module("rqdatac.services.future")
    basic_mod: types.ModuleType = _vendor_module("rqdatac.services.basic")
    calendar_mod: types.ModuleType = _vendor_module("rqdatac.services.calendar")
    _vendor_module("rqdatac.share")
    errors_mod: types.ModuleType = _vendor_module("rqdatac.share.errors")

    class RQDataError(Exception):
        pass

    errors_mod.RQDataError = RQDataError
    rqdatac.init = state.init
    get_price_mod.get_price = state.get_price
    future_mod.get_dominant_price = state.get_dominant_price
    basic_mod.all_instruments = state.all_instruments
    calendar_mod.get_next_trading_date = state.get_next_trading_date


def _preload_package(package_dir: Path) -> None:
    _purge(package_dir.name)
    package: types.ModuleType = types.ModuleType(package_dir.name)
    package.__path__ = [str(package_dir)]
    package.__package__ = package_dir.name
    sys.modules[package_dir.name] = package


STATE: _RqState = _RqState()
_install_rqdatac(STATE)
_preload_package(Path(__file__).resolve().parent.parent / "vnpy_rqdata")
rqdata = importlib.import_module("vnpy_rqdata.rqdata_datafeed")


def _frame(order_book_id: str, end_dt: datetime) -> pd.DataFrame:
    index: pd.MultiIndex = pd.MultiIndex.from_tuples(
        [(order_book_id, pd.Timestamp(end_dt))],
        names=["order_book_id", "datetime"],
    )
    return pd.DataFrame(
        {
            "open": [10.5],
            "high": [11.0],
            "low": [10.0],
            "close": [10.75],
            "volume": [100.0],
            "total_turnover": [1075.0],
            "open_interest": [20.0],
        },
        index=index,
    )


def _feed(symbols: list[str]) -> object:
    feed: object = rqdata.RqdataDatafeed()
    feed.inited = True
    feed.symbols = np.array(symbols)
    return feed


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    STATE.price_calls.clear()
    STATE.init_calls.clear()
    STATE.frame = None


@pytest.mark.parametrize(
    ("symbol", "exchange", "symbols", "expected"),
    [
        ("600009", Exchange.SSE, [], "600009.XSHG"),
        ("000001", Exchange.SZSE, [], "000001.XSHE"),
        ("rb2410", Exchange.SHFE, [], "RB2410"),
        ("IF2406", Exchange.CFFEX, [], "IF2406"),
        ("sc2412", Exchange.INE, [], "SC2412"),
        ("si2412", Exchange.GFEX, [], "SI2412"),
        ("TA501", Exchange.CZCE, ["TA2501"], "TA2501"),
        ("Au(T+D)", Exchange.SGE, [], "AUTD.SGEX"),
    ],
)
def test_to_rq_symbol(
    symbol: str,
    exchange: Exchange,
    symbols: list[str],
    expected: str,
) -> None:
    listed: ndarray = np.array(symbols)
    assert rqdata.to_rq_symbol(symbol, exchange, listed) == expected


def test_query_bar_history_stock_minute() -> None:
    STATE.frame = _frame("600009.XSHG", datetime(2024, 1, 2, 9, 31))
    feed = cast(rqdata.RqdataDatafeed, _feed(["600009.XSHG"]))
    req: HistoryRequest = HistoryRequest(
        symbol="600009",
        exchange=Exchange.SSE,
        start=datetime(2024, 1, 2, 9, 0),
        end=datetime(2024, 1, 2, 15, 0),
        interval=Interval.MINUTE,
    )

    bars: list[BarData] = feed.query_bar_history(req)

    assert STATE.init_calls == []
    assert len(STATE.price_calls) == 1
    args, kwargs = STATE.price_calls[0]
    assert args == ("600009.XSHG",)
    assert kwargs["frequency"] == "1m"
    assert kwargs["adjust_type"] == "pre_volume"
    assert len(bars) == 1
    bar: BarData = bars[0]
    assert bar.symbol == "600009"
    assert bar.exchange == Exchange.SSE
    assert bar.vt_symbol == "600009.SSE"
    assert bar.datetime == datetime(2024, 1, 2, 9, 30, tzinfo=rqdata.CHINA_TZ)
    assert bar.open_price == 10.5
    assert bar.high_price == 11.0
    assert bar.low_price == 10.0
    assert bar.close_price == 10.75
    assert bar.volume == 100.0


def test_query_bar_history_future_minute() -> None:
    STATE.frame = _frame("RB2410", datetime(2024, 1, 2, 21, 1))
    feed = cast(rqdata.RqdataDatafeed, _feed(["RB2410"]))
    req: HistoryRequest = HistoryRequest(
        symbol="rb2410",
        exchange=Exchange.SHFE,
        start=datetime(2024, 1, 2, 21, 0),
        end=datetime(2024, 1, 2, 23, 0),
        interval=Interval.MINUTE,
    )

    bars: list[BarData] = feed.query_bar_history(req)

    assert STATE.init_calls == []
    args, kwargs = STATE.price_calls[0]
    assert args == ("RB2410",)
    assert kwargs["frequency"] == "1m"
    assert kwargs["adjust_type"] == "none"
    assert kwargs["fields"] == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "total_turnover",
        "open_interest",
    ]
    assert len(bars) == 1
    bar: BarData = bars[0]
    assert bar.symbol == "rb2410"
    assert bar.exchange == Exchange.SHFE
    assert bar.vt_symbol == "rb2410.SHFE"
    assert bar.datetime == datetime(2024, 1, 2, 21, 0, tzinfo=rqdata.CHINA_TZ)
    assert bar.open_price == 10.5
    assert bar.high_price == 11.0
    assert bar.low_price == 10.0
    assert bar.close_price == 10.75
    assert bar.volume == 100.0


def test_unknown_symbol_reports_vt_symbol() -> None:
    feed = cast(rqdata.RqdataDatafeed, _feed(["600009.XSHG"]))
    logs: list[str] = []
    req: HistoryRequest = HistoryRequest(
        symbol="AAPL",
        exchange=Exchange.NYSE,
        start=datetime(2024, 1, 2, 9, 30),
        end=datetime(2024, 1, 2, 16, 0),
        interval=Interval.DAILY,
    )

    assert feed.query_bar_history(req, logs.append) == []
    assert STATE.price_calls == []
    assert STATE.init_calls == []
    assert logs == ["RQData查询K线数据失败：不支持的合约代码AAPL.NYSE"]
