import importlib
import sys
import types
from datetime import datetime
from pathlib import Path
from typing import cast

import pytest

from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange, Product
from vnpy.trader.object import ContractData, SubscribeRequest, TickData


class _RqApi:
    def init(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("rqdatac.init must not run")

    def all_instruments(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("rqdatac.all_instruments must not run")

    def get_price(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("rqdatac.get_price must not run")

    def get_dominant_price(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("rqdatac.get_dominant_price must not run")

    def get_next_trading_date(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("rqdatac.get_next_trading_date must not run")


class _LiveMarketDataClient:
    def subscribe(self, channel: str) -> None:
        raise RuntimeError("LiveMarketDataClient.subscribe must not run")

    def listen(self, handler: object) -> None:
        raise RuntimeError("LiveMarketDataClient.listen must not run")

    def close(self) -> None:
        raise RuntimeError("LiveMarketDataClient.close must not run")


class _RecordingClient:
    def __init__(self) -> None:
        self.channels: list[str] = []

    def subscribe(self, channel: str) -> None:
        self.channels.append(channel)


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


def _install_rqdatac(api: _RqApi) -> None:
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
    rqdatac.init = api.init
    rqdatac.all_instruments = api.all_instruments
    rqdatac.LiveMarketDataClient = _LiveMarketDataClient
    get_price_mod.get_price = api.get_price
    future_mod.get_dominant_price = api.get_dominant_price
    basic_mod.all_instruments = api.all_instruments
    calendar_mod.get_next_trading_date = api.get_next_trading_date


def _preload_package(package_dir: Path) -> None:
    _purge(package_dir.name)
    package: types.ModuleType = types.ModuleType(package_dir.name)
    package.__path__ = [str(package_dir)]
    package.__package__ = package_dir.name
    sys.modules[package_dir.name] = package


_API: _RqApi = _RqApi()
_install_rqdatac(_API)
_preload_package(Path(__file__).resolve().parent.parent / "vnpy_rqdata")
rq_gateway = importlib.import_module("vnpy_rqdata.rqdata_gateway")
_package: types.ModuleType = sys.modules["vnpy_rqdata"]
if any(hasattr(_package, name) for name in ("RqdataGateway", "Datafeed", "__version__")):
    raise RuntimeError("vnpy_rqdata package init must not run")


def _new_gateway() -> rq_gateway.RqdataGateway:
    return rq_gateway.RqdataGateway(EventEngine(), "RQDATA")


def _contract(symbol: str, exchange: Exchange, name: str) -> ContractData:
    return ContractData(
        symbol=symbol,
        exchange=exchange,
        name=name,
        product=Product.EQUITY,
        size=1,
        pricetick=0.01,
        gateway_name="RQDATA",
    )


def _capture_ticks(gw: rq_gateway.RqdataGateway) -> list[TickData]:
    ticks: list[TickData] = []

    def _on_tick(tick: TickData) -> None:
        ticks.append(tick)

    gw.on_tick = _on_tick
    return ticks


def _quote(order_book_id: str) -> dict[str, object]:
    return {
        "order_book_id": order_book_id,
        "datetime": "20240102093000000000",
        "prev_close": 10.0,
        "volume": 1000.0,
        "total_turnover": 10500.0,
        "open_interest": 20.0,
        "last": 10.5,
        "limit_up": 11.0,
        "limit_down": 9.0,
        "open": 10.1,
        "high": 10.6,
        "low": 9.8,
        "bid": [10.4, 10.3, 10.2, 10.1, 10.0],
        "ask": [10.5, 10.6, 10.7, 10.8, 10.9],
        "bid_vol": [1.0, 2.0, 3.0, 4.0, 5.0],
        "ask_vol": [6.0, 7.0, 8.0, 9.0, 10.0],
    }


@pytest.mark.parametrize(
    ("symbol", "exchange", "channel"),
    [
        ("600009", Exchange.SSE, "tick_600009.XSHG"),
        ("000001", Exchange.SZSE, "tick_000001.XSHE"),
    ],
)
def test_subscribe_stock_without_client(
    symbol: str,
    exchange: Exchange,
    channel: str,
) -> None:
    gw: rq_gateway.RqdataGateway = _new_gateway()

    gw.subscribe(SubscribeRequest(symbol=symbol, exchange=exchange))

    assert gw.client is None
    assert gw.subscribed == {channel}
    assert gw.futures_map == {}


def test_subscribe_future_writes_futures_map() -> None:
    gw: rq_gateway.RqdataGateway = _new_gateway()

    gw.subscribe(SubscribeRequest(symbol="rb2410", exchange=Exchange.SHFE))

    assert gw.client is None
    assert gw.futures_map == {"RB2410": ("rb2410", Exchange.SHFE)}
    assert gw.subscribed == {"tick_RB2410"}


def test_handle_msg_known_order_book_id_emits_tick() -> None:
    gw: rq_gateway.RqdataGateway = _new_gateway()
    gw.symbol_map["600009.XSHG"] = _contract("600009", Exchange.SSE, "stock")
    ticks: list[TickData] = _capture_ticks(gw)

    gw.handle_msg(_quote("600009.XSHG"))

    assert len(ticks) == 1
    tick: TickData = ticks[0]
    assert tick.symbol == "600009"
    assert tick.exchange == Exchange.SSE
    assert tick.name == "stock"
    assert tick.datetime == datetime(2024, 1, 2, 9, 30, tzinfo=rq_gateway.CHINA_TZ)
    assert tick.volume == 1000.0
    assert tick.turnover == 10500.0
    assert tick.open_interest == 20.0
    assert tick.last_price == 10.5
    assert tick.limit_up == 11.0
    assert tick.limit_down == 9.0
    assert tick.open_price == 10.1
    assert tick.high_price == 10.6
    assert tick.low_price == 9.8
    assert tick.pre_close == 10.0
    assert tick.gateway_name == "RQDATA"
    assert tick.bid_price_1 == 10.4
    assert tick.bid_price_2 == 10.3
    assert tick.bid_price_3 == 10.2
    assert tick.bid_price_4 == 10.1
    assert tick.bid_price_5 == 10.0
    assert tick.ask_price_1 == 10.5
    assert tick.ask_price_2 == 10.6
    assert tick.ask_price_3 == 10.7
    assert tick.ask_price_4 == 10.8
    assert tick.ask_price_5 == 10.9
    assert tick.bid_volume_1 == 1.0
    assert tick.bid_volume_2 == 2.0
    assert tick.bid_volume_3 == 3.0
    assert tick.bid_volume_4 == 4.0
    assert tick.bid_volume_5 == 5.0
    assert tick.ask_volume_1 == 6.0
    assert tick.ask_volume_2 == 7.0
    assert tick.ask_volume_3 == 8.0
    assert tick.ask_volume_4 == 9.0
    assert tick.ask_volume_5 == 10.0


def test_handle_msg_unknown_order_book_id_emits_no_tick() -> None:
    gw: rq_gateway.RqdataGateway = _new_gateway()
    gw.symbol_map["600009.XSHG"] = _contract("600009", Exchange.SSE, "stock")
    ticks: list[TickData] = _capture_ticks(gw)

    gw.handle_msg({"order_book_id": "000001.XSHE"})

    assert ticks == []


def test_subscribe_with_client_calls_subscribe() -> None:
    gw: rq_gateway.RqdataGateway = _new_gateway()
    client: _RecordingClient = _RecordingClient()
    gw.client = cast(rq_gateway.LiveMarketDataClient, client)

    gw.subscribe(SubscribeRequest(symbol="600009", exchange=Exchange.SSE))

    assert client.channels == ["tick_600009.XSHG"]
    assert gw.subscribed == {"tick_600009.XSHG"}
