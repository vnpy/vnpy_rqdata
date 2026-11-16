from bisect import bisect_right
from datetime import date, datetime, timedelta
from typing import cast
from collections.abc import Callable

from numpy import ndarray
from pandas import DataFrame, Timestamp
from rqdatac import init
from rqdatac.services.get_price import get_price
from rqdatac.services.future import get_dominant_price
from rqdatac.services.basic import all_instruments
from rqdatac.services.calendar import get_next_trading_date
from rqdatac.share.errors import RQDataError

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import round_to, ZoneInfo
from vnpy.trader.datafeed import BaseDatafeed


INTERVAL_VT2RQ: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "60m",
    Interval.DAILY: "1d",
}

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()         # no need to adjust for daily bar
}

FUTURES_EXCHANGES: set[Exchange] = {
    Exchange.CFFEX,
    Exchange.SHFE,
    Exchange.CZCE,
    Exchange.DCE,
    Exchange.INE,
    Exchange.GFEX
}

FUND_TYPES: set[str] = {"ETF", "LOF", "REITs"}
EX_FACTOR_TYPES: set[str] = {"CS", "ETF", "LOF", "REITs"}
NAV_LOOKBACK_DAYS: int = 15

CHINA_TZ = ZoneInfo("Asia/Shanghai")


def to_rq_symbol(symbol: str, exchange: Exchange, all_symbols: ndarray) -> str:
    """将交易所代码转换为米筐代码"""
    # 股票
    if exchange in {Exchange.SSE, Exchange.SZSE}:
        if exchange == Exchange.SSE:
            rq_symbol: str = f"{symbol}.XSHG"
        else:
            rq_symbol = f"{symbol}.XSHE"
    # 金交所现货
    elif exchange == Exchange.SGE:
        for char in ["(", ")", "+"]:
            symbol = symbol.replace(char, "")
        symbol = symbol.upper()
        rq_symbol = f"{symbol}.SGEX"
    # 期货和期权
    elif exchange in {
        Exchange.CFFEX,
        Exchange.SHFE,
        Exchange.DCE,
        Exchange.CZCE,
        Exchange.INE,
        Exchange.GFEX
    }:
        for count, word in enumerate(symbol):  # noqa: B007
            if word.isdigit():
                break

        product: str = symbol[:count]
        time_str: str = symbol[count:]

        # 期货
        if time_str.isdigit():
            # 只有郑商所需要特殊处理
            if exchange is not Exchange.CZCE:
                return symbol.upper()

            # 检查是否为连续合约或者指数合约
            if time_str in ["88", "888", "99", "889"]:
                return symbol

            # 提取年月
            year: str = symbol[count]
            month: str = symbol[count + 1:]

            guess_1: str = f"{product}1{year}{month}".upper()
            guess_2: str = f"{product}2{year}{month}".upper()

            # 优先尝试20年后的合约
            if guess_2 in all_symbols:
                rq_symbol = guess_2
            else:
                rq_symbol = guess_1
        # 期权以及期货次主力连续合约
        else:
            if time_str == "88A2":
                return symbol.upper()

            if exchange in {
                Exchange.CFFEX,
                Exchange.DCE,
                Exchange.SHFE,
                Exchange.INE,
                Exchange.GFEX
            }:
                rq_symbol = symbol.replace("-", "").upper()
            elif exchange == Exchange.CZCE:
                year = symbol[count]
                suffix: str = symbol[count + 1:]

                guess_1 = f"{product}1{year}{suffix}".upper()
                guess_2 = f"{product}2{year}{suffix}".upper()

                # 优先尝试20年后的合约
                if guess_2 in all_symbols:
                    rq_symbol = guess_2
                else:
                    rq_symbol = guess_1
    else:
        rq_symbol = f"{symbol}.{exchange.value}"

    return rq_symbol


class RqdataDatafeed(BaseDatafeed):
    """米筐RQData数据服务接口"""

    def __init__(self) -> None:
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.inited: bool = False

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True

        if not self.username:
            output("RQData数据服务初始化失败：用户名为空！")
            return False

        if not self.password:
            output("RQData数据服务初始化失败：密码为空！")
            return False

        try:
            init(
                self.username,
                self.password,
                ("rqdatad-pro.ricequant.com", 16011),
                use_pool=True,
                max_pool_size=1,
                auto_load_plugins=False
            )

            df: DataFrame = all_instruments()
            self.symbols: ndarray = df["order_book_id"].to_numpy()
            self.instrument_types: dict[str, str] = dict(
                zip(df["order_book_id"].astype(str), df["type"].astype(str), strict=True)
            )
        except RQDataError as ex:
            output(f"RQData数据服务初始化失败：{ex}")
            return False
        except RuntimeError as ex:
            output(f"发生运行时错误：{ex}")
            return False
        except Exception as ex:
            output(f"发生未知异常：{ex}")
            return False

        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """查询K线数据"""
        # 期货品种且代码中没有数字（非具体合约），则查询主力连续
        if req.exchange in FUTURES_EXCHANGES and req.symbol.isalpha():
            return self._query_dominant_history(req, output)
        else:
            return self._query_bar_history(req, output)

    def _query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        # 股票期权不添加交易所后缀
        if exchange in [Exchange.SSE, Exchange.SZSE] and symbol in self.symbols:
            rq_symbol: str = symbol
        else:
            rq_symbol = to_rq_symbol(symbol, exchange, self.symbols)

        # 检查查询的代码在范围内
        if rq_symbol not in self.symbols:
            output(f"RQData查询K线数据失败：不支持的合约代码{req.vt_symbol}")
            return []

        rq_interval: str | None = INTERVAL_VT2RQ.get(interval, None)
        if not rq_interval:
            output(f"RQData查询K线数据失败：不支持的时间周期{req.interval.value}")
            return []

        # 为了将米筐时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]

        # 只对衍生品合约才查询持仓量数据
        fields: list = ["open", "high", "low", "close", "volume", "total_turnover"]

        if not symbol.isdigit():
            fields.append("open_interest")
        elif (
            symbol.startswith("51")         # 上交所ETF
            or symbol.startswith("58")      # 科创版ETF
            or symbol.startswith("159")     # 深交所ETF
        ):
            fields.append("iopv")

        # 对于股票查询前复权K线数据
        if rq_symbol.endswith(".XSHG") or rq_symbol.endswith(".XSHE"):
            adjust_type: str = "pre_volume"
        else:
            adjust_type = "none"

        df: DataFrame = get_price(
            rq_symbol,
            frequency=rq_interval,
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),        # 为了查询夜盘数据
            adjust_type=adjust_type
        )

        data: list[BarData] = []
        has_iopv: bool = "iopv" in df.columns

        if df is not None:
            # 填充NaN为0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                row_index: tuple[str, Timestamp] = cast(tuple[str, Timestamp], row.Index)
                dt: datetime = row_index[1].to_pydatetime() - adjustment
                dt = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.open, 0.000001),
                    high_price=round_to(row.high, 0.000001),
                    low_price=round_to(row.low, 0.000001),
                    close_price=round_to(row.close, 0.000001),
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="RQ"
                )

                if has_iopv:
                    bar.extra = {"iopv": row.iopv}

                data.append(bar)

        if data and (rq_symbol.endswith(".XSHG") or rq_symbol.endswith(".XSHE")):
            self._attach_security_extra(symbol, rq_symbol, start, end, data, output)

        return data

    def _attach_security_extra(
        self,
        symbol: str,
        rq_symbol: str,
        start: datetime,
        end: datetime,
        bars: list[BarData],
        output: Callable,
    ) -> None:
        """为沪深证券K线附加净值和除权因子"""
        instrument_type: str = self.instrument_types.get(rq_symbol, "")
        nav: dict[date, float] | None = None
        factor: dict[date, tuple[float, float]] | None = None

        if instrument_type in FUND_TYPES:
            nav = self._load_nav(symbol, rq_symbol, start, end, output)
        if instrument_type in EX_FACTOR_TYPES:
            factor = self._load_ex_factor(rq_symbol, end, output)
        if not nav and not factor:
            return

        nav_series: dict[date, float] = nav or {}
        factor_series: dict[date, tuple[float, float]] = factor or {}
        nav_dates: list[date] = sorted(nav_series)
        ex_dates: list[date] = sorted(factor_series)

        for bar in bars:
            day: date = bar.datetime.date()
            extra: dict = dict(bar.extra) if bar.extra else {}

            if nav_dates:
                nav_ix: int = bisect_right(nav_dates, day) - 1
                if nav_ix >= 0:
                    nav_date: date = nav_dates[nav_ix]
                    extra["nav"] = {
                        "unit_net_value": nav_series[nav_date],
                        "date": nav_date.strftime("%Y-%m-%d"),
                    }

            if ex_dates:
                ex_ix: int = bisect_right(ex_dates, day) - 1
                if ex_ix >= 0:
                    ex_date: date = ex_dates[ex_ix]
                    ex_factor, ex_cum_factor = factor_series[ex_date]
                    extra["ex_factor"] = {
                        "ex_factor": ex_factor,
                        "ex_cum_factor": ex_cum_factor,
                        "date": ex_date.strftime("%Y-%m-%d"),
                    }

            if extra:
                bar.extra = extra

    def _load_nav(
        self,
        symbol: str,
        rq_symbol: str,
        start: datetime,
        end: datetime,
        output: Callable,
    ) -> dict[date, float] | None:
        """读取基金净值序列"""
        try:
            import rqdatac_fund
            from pandas import to_datetime

            df: DataFrame = rqdatac_fund.fund.get_nav(
                [symbol],
                start_date=(start - timedelta(days=NAV_LOOKBACK_DAYS)).date(),
                end_date=end.date(),
                fields=["unit_net_value"],
                expect_df=True,
            )
        except Exception as ex:
            output(f"RQData基金净值查询失败：{rq_symbol} {ex}")
            return None

        if df is None or df.empty or "unit_net_value" not in df.columns:
            return None

        df = df.dropna(subset=["unit_net_value"])
        if df.empty:
            return None

        series: dict[date, float] = {}
        for index, value in df["unit_net_value"].items():
            if not isinstance(index, tuple):
                continue

            nav_date: date = to_datetime(index[1]).date()
            series[nav_date] = float(value)

        return series or None

    def _load_ex_factor(
        self,
        rq_symbol: str,
        end: datetime,
        output: Callable,
    ) -> dict[date, tuple[float, float]] | None:
        """读取截至结束日的除权因子。记录按除权日给出，早于区间的最近一条仍然有效。"""
        try:
            from pandas import to_datetime
            from rqdatac import get_ex_factor

            df: DataFrame = get_ex_factor(rq_symbol, end_date=end.date())
        except Exception as ex:
            output(f"RQData除权因子查询失败：{rq_symbol} {ex}")
            return None

        if df is None or df.empty or "ex_factor" not in df.columns:
            return None

        df = df.dropna(subset=["ex_factor", "ex_cum_factor"])
        if df.empty:
            return None

        series: dict[date, tuple[float, float]] = {}
        for index, row in df.iterrows():
            ts = index[-1] if isinstance(index, tuple) else index
            ex_date: date = to_datetime(ts).date()
            series[ex_date] = (float(row.ex_factor), float(row.ex_cum_factor))

        return series or None

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData] | None:
        """查询Tick数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        start: datetime = req.start
        end: datetime = req.end

        # 股票期权不添加交易所后缀
        if exchange in [Exchange.SSE, Exchange.SZSE] and symbol in self.symbols:
            rq_symbol: str = symbol
        else:
            rq_symbol = to_rq_symbol(symbol, exchange, self.symbols)

        if rq_symbol not in self.symbols:
            output(f"RQData查询Tick数据失败：不支持的合约代码{req.vt_symbol}")
            return []

        # 只对衍生品合约才查询持仓量数据
        fields: list = [
            "open",
            "high",
            "low",
            "last",
            "prev_close",
            "volume",
            "total_turnover",
            "limit_up",
            "limit_down",
            "b1",
            "b2",
            "b3",
            "b4",
            "b5",
            "a1",
            "a2",
            "a3",
            "a4",
            "a5",
            "b1_v",
            "b2_v",
            "b3_v",
            "b4_v",
            "b5_v",
            "a1_v",
            "a2_v",
            "a3_v",
            "a4_v",
            "a5_v",
        ]
        if not symbol.isdigit():
            fields.append("open_interest")

        df: DataFrame = get_price(
            rq_symbol,
            frequency="tick",
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),        # 为了查询夜盘数据
            adjust_type="none"
        )

        data: list[TickData] = []

        if df is not None:
            # 填充NaN为0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                row_index: tuple[str, Timestamp] = cast(tuple[str, Timestamp], row.Index)
                dt: datetime = row_index[1].to_pydatetime()
                dt = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                tick: TickData = TickData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=dt,
                    open_price=row.open,
                    high_price=row.high,
                    low_price=row.low,
                    pre_close=row.prev_close,
                    last_price=row.last,
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    limit_up=row.limit_up,
                    limit_down=row.limit_down,
                    bid_price_1=row.b1,
                    bid_price_2=row.b2,
                    bid_price_3=row.b3,
                    bid_price_4=row.b4,
                    bid_price_5=row.b5,
                    ask_price_1=row.a1,
                    ask_price_2=row.a2,
                    ask_price_3=row.a3,
                    ask_price_4=row.a4,
                    ask_price_5=row.a5,
                    bid_volume_1=row.b1_v,
                    bid_volume_2=row.b2_v,
                    bid_volume_3=row.b3_v,
                    bid_volume_4=row.b4_v,
                    bid_volume_5=row.b5_v,
                    ask_volume_1=row.a1_v,
                    ask_volume_2=row.a2_v,
                    ask_volume_3=row.a3_v,
                    ask_volume_4=row.a4_v,
                    ask_volume_5=row.a5_v,
                    gateway_name="RQ"
                )

                data.append(tick)

        return data

    def _query_dominant_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """查询期货主力K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        rq_interval: str | None = INTERVAL_VT2RQ.get(interval, None)
        if not rq_interval:
            output(f"RQData查询K线数据失败：不支持的时间周期{req.interval.value}")
            return []

        # 为了将米筐时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[interval]

        # 只对衍生品合约才查询持仓量数据
        fields: list = ["open", "high", "low", "close", "volume", "total_turnover"]
        if not symbol.isdigit():
            fields.append("open_interest")

        df: DataFrame = get_dominant_price(
            symbol.upper(),                         # 合约代码用大写
            frequency=rq_interval,
            fields=fields,
            start_date=start,
            end_date=get_next_trading_date(end),    # 为了查询夜盘数据
            adjust_type="pre",                      # 前复权
            adjust_method="prev_close_ratio"        # 切换前一日收盘价比例复权
        )

        data: list[BarData] = []

        if df is not None:
            # 填充NaN为0
            df.fillna(0, inplace=True)

            for row in df.itertuples():
                row_index: tuple[str, Timestamp] = cast(tuple[str, Timestamp], row.Index)
                dt: datetime = row_index[1].to_pydatetime() - adjustment
                dt = dt.replace(tzinfo=CHINA_TZ)

                if dt >= end:
                    break

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.open, 0.000001),
                    high_price=round_to(row.high, 0.000001),
                    low_price=round_to(row.low, 0.000001),
                    close_price=round_to(row.close, 0.000001),
                    volume=row.volume,
                    turnover=row.total_turnover,
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="RQ"
                )

                data.append(bar)

        return data
