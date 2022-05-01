import datetime

from ttb.cfg.config import Config
from ttb.util import timeutil


class TradeControl:
    def __init__(self, config=None):
        self.conf = config or Config()
        self.default_qty = self.conf.trade_default_qty
        self.per_trade_amt_limit = self.conf.per_trade_amt_limit
        self.daily_trade_amt_limit = self.conf.daily_trade_amt_limit
        self.buy_start_time = timeutil.parse_time(self.conf.buy_start_time)
        self.buy_end_time = timeutil.parse_time(self.conf.buy_end_time)

    def __within_daily_limit(self, amt: float):
        return amt < self.daily_trade_amt_limit

    def __during_buy_period(self):
        dt = datetime.datetime.now().timestamp()
        return dt > self.buy_start_time.timestamp() and dt < self.buy_end_time.timestamp()

    def safe_to_buy(self, total_buy_amt: float):
        return self.__within_daily_limit(total_buy_amt) and self.__during_buy_period()
