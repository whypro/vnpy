from datetime import timedelta, datetime
from typing import List, Optional
from pytz import timezone

import tushare as ts

from .setting import SETTINGS
from .constant import Exchange, Interval
from .object import BarData, HistoryRequest

INTERVAL_VT2TS = {
    Interval.MINUTE: "1min",
    Interval.HOUR: "60min",
    Interval.DAILY: "D",
}

INTERVAL_ADJUSTMENT_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()  # no need to adjust for daily bar
}

CHINA_TZ = timezone("Asia/Shanghai")


class TushareClient:
    """
    Client for querying history data from Tushare.
    """

    def __init__(self):
        """"""
        self.token: str = SETTINGS["tushare.token"]

        self.inited: bool = False
        self.symbols: set = set()

    def init(self, token: str = "") -> bool:
        """"""
        if self.inited:
            return True

        if token:
            self.token = token

        if not self.token:
            return False

        ts.set_token(self.token)
        self.inited = True
        return True

    def to_ts_code(self, symbol: str, exchange: Exchange) -> (str, str):
        """
        CZCE product of RQData has symbol like "TA1905" while
        vt symbol is "TA905.CZCE" so need to add "1" in symbol.
        """
        asset_code = ''
        # 期货
        if exchange == Exchange.CZCE:
            ts_code = f"{symbol}.ZCE"
            asset_code = 'FT'
        elif exchange == Exchange.SHFE:
            ts_code = f"{symbol}.SHF"
            asset_code = 'FT'
        elif exchange == Exchange.DCE:
            ts_code = f"{symbol}.DCE"
            asset_code = 'FT'
        elif exchange == Exchange.CFFEX:
            ts_code = f"{symbol}.CFX"
            asset_code = 'FT'
        elif exchange == Exchange.INE:
            ts_code = f"{symbol}.INE"
            asset_code = 'FT'
        elif exchange == Exchange.CZCE:
            ts_code = f"{symbol}.ZCE"
        # 股票
        elif exchange == Exchange.SSE:
            ts_code = f"{symbol}.SH"
            if symbol.startswith('000'):
                asset_code = 'I'
            elif symbol.startswith('500') or symbol.startswith('550'):
                asset_code = 'FD'
            elif symbol.startswith('600') or symbol.startswith('601') or \
                    symbol.startswith('603') or symbol.startswith('688') or symbol.startswith('900'):
                asset_code = 'E'
        elif exchange == Exchange.SZSE:
            ts_code = f"{symbol}.SZ"
            if symbol.startswith('00') or symbol.startswith('200'):
                asset_code = 'E'
            elif symbol.startswith('17') or symbol.startswith('18'):
                asset_code = 'FD'
            elif symbol.startswith('39'):
                asset_code = 'I'
        else:
            ts_code = f"{symbol}.{exchange.value}"
        return ts_code, asset_code

    def query_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """
        Query history bar data from RQData.
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        ts_code, asset_code = self.to_ts_code(symbol, exchange)
        # if ts_code not in self.symbols:
        #    return None

        freq = INTERVAL_VT2TS.get(interval)
        if not freq:
            return None

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        # adjustment = INTERVAL_ADJUSTMENT_MAP[interval]

        # For querying night trading period data
        # end += timedelta(1)

        df = ts.pro_bar(
            ts_code=ts_code,
            start_date=start.strftime('%Y%m%d'),
            end_date=end.strftime('%Y%m%d'),
            freq=freq,
            asset=asset_code,
        )

        data: List[BarData] = []

        if df is not None:
            for ix, row in df.iterrows():
                dt = datetime.strptime(row["trade_date"], "%Y%m%d").astimezone(CHINA_TZ)
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["amount"],
                    gateway_name="TS"
                )
                data.append(bar)

        return data


tushare_client = TushareClient()
