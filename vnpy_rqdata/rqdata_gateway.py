from threading import Thread
from typing import Dict, List, Set, Tuple
from datetime import datetime

from pandas import DataFrame
from rqdatac import (
    LiveMarketDataClient,
    init,
    all_instruments
)

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.constant import Exchange, Product
from vnpy.trader.object import (
    SubscribeRequest,
    OrderRequest,
    CancelRequest,
    TickData,
    ContractData
)
from vnpy.trader.utility import ZoneInfo


CHINA_TZ = ZoneInfo("Asia/Shanghai")


EXCHANGE_VT2RQDATA = {
    Exchange.SSE: "XSHG",
    Exchange.SZSE: "XSHE",
    Exchange.CFFEX: "CFFEX",
    Exchange.SHFE: "SHFE",
    Exchange.DCE: "DCE",
    Exchange.CZCE: "CZCE",
    Exchange.INE: "INE",
    Exchange.GFEX: "GFEX"
}
EXCHANGE_RQDATA2VT = {v: k for k, v in EXCHANGE_VT2RQDATA.items()}


PRODUCT_MAP = {
    "CS": Product.EQUITY,
    "INDX": Product.INDEX,
    "ETF": Product.FUND,
    "LOF": Product.FUND,
    "FUND": Product.FUND,
    "Future": Product.FUTURES,
    "Option": Product.OPTION,
    "Convertible": Product.BOND,
    "Repo": Product.BOND
}


class RqdataGateway(BaseGateway):
    """
    VeighNa框架用于对接RQData实时行情的接口。
    """

    default_name: str = "RQDATA"

    default_setting: Dict[str, str] = {
        "用户名": "",
        "密码": ""
    }

    exchanges: List[str] = list(EXCHANGE_VT2RQDATA.keys())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        super().__init__(event_engine, gateway_name)

        self.client: LiveMarketDataClient = None
        self.thread: Thread = None

        self.subscribed: Set[str] = set()
        self.futures_map: Dict[str, Tuple[str, Exchange]] = {}      # 期货代码交易所映射信息
        self.symbol_map: Dict[str, str] = {}

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        if self.client:
            return

        # 初始化rqdatac
        username: str = setting["用户名"]
        password: str = setting["密码"]

        try:
            init(username, password)
        except Exception as ex:
            self.write_log(f"RQData接口初始化失败：{ex}")
            return

        # 查询合约信息
        self.query_contract()

        # 创建实时行情客户端
        self.client = LiveMarketDataClient()

        # 启动运行线程
        self.thread = self.client.listen(handler=self.handle_msg)

        # 订阅之前行情
        for rq_channel in self.subscribed:
            self.clicent.subscrbie(rq_channel)

        self.write_log("RQData接口初始化成功")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        # 证券
        if req.exchange in {Exchange.SSE, Exchange.SZSE}:
            rq_exchange: str = EXCHANGE_VT2RQDATA[req.exchange]
            rq_channel: str = f"tick_{req.symbol}.{rq_exchange}"
        # 期货
        else:
            rq_symbol: str = req.symbol.upper()
            rq_channel: str = f"tick_{rq_symbol}"

            self.futures_map[rq_symbol] = (req.symbol, req.exchange)

        self.subscribed.add(rq_channel)

        if self.client:
            self.client.subscribe(rq_channel)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return ""

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        pass

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def close(self) -> None:
        """关闭接口"""
        if self.client:
            self.client.close()
            self.thread.join()

    def query_contract(self) -> None:
        """查询合约"""
        for t in ["CS", "INDX", "ETF", "Future"]:
            df: DataFrame = all_instruments(type=t)

            for tp in df.itertuples():
                if t == "INDX":
                    symbol, rq_exchange = tp.order_book_id.split(".")
                    exchange: Exchange = EXCHANGE_RQDATA2VT.get(rq_exchange, None)
                else:
                    symbol: str = tp.trading_code
                    exchange: Exchange = EXCHANGE_RQDATA2VT.get(tp.exchange, None)

                if not exchange:
                    continue

                min_volume: float = tp.round_lot

                product: Product = PRODUCT_MAP[tp.type]
                if product == Product.EQUITY:
                    size: int = 1
                    pricetick: float = 0.01
                    product_name: str = "股票"
                elif product == Product.FUND:
                    size: int = 1
                    pricetick: float = 0.001
                    product_name: str = "基金"
                elif product == Product.INDEX:
                    size: int = 1
                    pricetick: float = 0.01
                    product_name: str = "指数"
                elif product == Product.FUTURES:
                    size: int = tp.contract_multiplier
                    pricetick: float = 0.01
                    product_name: str = "期货"

                contract = ContractData(
                    symbol=symbol,
                    exchange=exchange,
                    name=tp.symbol,
                    product=product,
                    size=size,
                    pricetick=pricetick,
                    min_volume=min_volume,
                    gateway_name=self.gateway_name
                )
                self.on_contract(contract)

                self.symbol_map[tp.order_book_id] = contract

            self.write_log(f"{product_name}合约信息查询成功")

    def handle_msg(self, data: dict) -> None:
        """处理行情推送"""
        contract: ContractData = self.symbol_map.get(data["order_book_id"], None)
        if not contract:
            self.write_log(f"收到不支持合约{data['order_book_id']}的行情推送")
            return

        dt: datetime = datetime.strptime(str(data["datetime"]), "%Y%m%d%H%M%S%f")
        dt = dt.replace(tzinfo=CHINA_TZ)
        tick: TickData = TickData(
            symbol=contract.symbol,
            exchange=contract.exchange,
            name=contract.name,
            datetime=dt,
            volume=data["volume"],
            turnover=data["total_turnover"],
            open_interest=data.get("open_interest", 0),
            last_price=data["last"],
            limit_up=data.get("limit_up", 0),
            limit_down=data.get("limit_down", 0),
            open_price=data["open"],
            high_price=data["high"],
            low_price=data["low"],
            pre_close=data["prev_close"],
            gateway_name=self.gateway_name
        )

        if "bid" in data:
            bp: List[float] = data["bid"]
            ap: List[float] = data["ask"]
            bv: List[float] = data["bid_vol"]
            av: List[float] = data["ask_vol"]

            tick.bid_price_1 = bp[0]
            tick.bid_price_2 = bp[1]
            tick.bid_price_3 = bp[2]
            tick.bid_price_4 = bp[3]
            tick.bid_price_5 = bp[4]

            tick.ask_price_1 = ap[0]
            tick.ask_price_2 = ap[1]
            tick.ask_price_3 = ap[2]
            tick.ask_price_4 = ap[3]
            tick.ask_price_5 = ap[4]

            tick.bid_volume_1 = bv[0]
            tick.bid_volume_2 = bv[1]
            tick.bid_volume_3 = bv[2]
            tick.bid_volume_4 = bv[3]
            tick.bid_volume_5 = bv[4]

            tick.ask_volume_1 = av[0]
            tick.ask_volume_2 = av[1]
            tick.ask_volume_3 = av[2]
            tick.ask_volume_4 = av[3]
            tick.ask_volume_5 = av[4]

        self.on_tick(tick)
