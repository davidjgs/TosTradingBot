import time

from ttb.cfg.config import Config
from ttb.control.trading_control import TradeControl
from ttb.data.event_type import EventType
from ttb.data.pnl_type import PnlType
from ttb.event.inbound.rpa_alert_reader import AlertReader
from ttb.event.outbound.to_file_event import ToFileEventHandler
from ttb.report.pnl_report import PnlReporter
from ttb.trading.td_client import TosTrader
from queue import Queue, Empty
import datetime
import threading

from ttb.util import timeutil
from ttb.util.app_logging import getLogger_rpa

logger = getLogger_rpa('ttb.main.bot_manager_rpa')


class BotManager:

    def __init__(self, config=None):
        self.__conf = config or Config()
        self.__event_q = Queue()
        self.__ticker_q = Queue()
        self.__hist_price_q = Queue()
        self.__default_qty = self.__conf.trade_default_qty
        self.__per_trade_amt_limit = self.__conf.per_trade_amt_limit
        self.__daily_trade_amt_limit = self.__conf.daily_trade_amt_limit
        self.__total_amt = 0
        self.trading_end_time = timeutil.parse_time(self.__conf.trade_end_time)
        self.__long_positions = {}
        self.__executions = {}
        self.__pnl = {}
        self.__persister = None  ##DBPersister()
        self.__alert_reader = AlertReader(self.__conf, self.__event_q, self.__persister)
        self.__trader = TosTrader(self.__conf)
        self.__event_handler = ToFileEventHandler(self.__conf)
        self.__reporter = PnlReporter(config=self.__conf)
        self.trade_control = TradeControl(self.__conf)

    def start(self):

        print('Starting BotManager ...')
        logger.info('Starting BotManager ...')

        thread = threading.Thread(target=self.__alert_reader.start)
        thread.start()
        ##threading.Thread(target=self.__priceAnalyzer.start).start()
        self.work()

    def work(self):
        print('Start working ...')
        logger.info('Start working ...')
        while datetime.datetime.now().timestamp() < self.trading_end_time.timestamp():
            while not self.__event_q.empty():
                event = self.__event_q.get()
                self.__on_event(event)
            time.sleep(5)
        self.eod_process()
        self.gen_reports()

    def __on_event(self, event):
        (symbols, action, version, ts) = event
        self.publish_event(
            {"event_type": EventType.RPA_ALERT, "tickers": symbols, "action": action,
             "version": version, "ts": ts})
        next_move = self.next_move(action)
        source = f'#B4#{version}#'
        if next_move:
            print(f'processing event {event}')
            logger.info(f'processing event {event}')
            side = next_move
            self.trade(symbols, side, version, source, ts)
            self.show_statistics()
        else:
            logger.info(f'event ignored :{event}')

    def next_move(self, action):
        if action == "BUY": ## and self.trade_control.safe_to_buy(self.__total_amt):
            return "BUY"
        elif action == "SELL":
            return "SELL"
        return None

    def trade(self, symbols, side, version, source, ts):
        if symbols:
            for ticker in symbols:
                dt = datetime.datetime.now()
                exec_time = dt.strftime("%Y/%m/%d-%H:%M:%S")
                if side == 'BUY' and ticker not in self.__long_positions:
                    price_b = self.execute_buy(ticker)
                    if price_b:
                        buy_qty = self.calc_qty(price_b)
                        if buy_qty:
                            self.__long_positions.setdefault(version, dict())[ticker] = {'price': float(price_b),
                                                                                         'qty': buy_qty,
                                                                                         'exec_time': exec_time,
                                                                                         'alert_buy_ts': ts,
                                                                                         'scanner': source}  # (float(price_b), exec_time)
                            # self.__priceAnalyzer.add_price(ticker, price_b, dt.strftime('%H%M%S'))
                            execution_buy = dict(
                                event_type=EventType.TRADE,
                                ticker=ticker,
                                price=price_b,
                                qty=buy_qty,
                                side=side,
                                exec_time=exec_time,
                                alert_buy_ts=ts,
                                scanner=source,
                            )
                            # self.__persister.insert_execution(execution_buy)
                            self.__total_amt += price_b*buy_qty
                            self.__ticker_q.put(ticker)
                            self.publish_event(execution_buy)
                        else:
                            logger.info(f"# of shares < 1, skipped buy for : {ticker}")
                    else:
                        logger.error(f'failed to execute buy for : {ticker}')
                elif side == 'SELL' and version in self.__long_positions and ticker in self.__long_positions[version]:
                    price_s = self.execute_sell(ticker)
                    if price_s:
                        price_sold = float(price_s)
                        exec_b = self.__long_positions[version].pop(ticker)
                        if len(self.__long_positions[version]) == 0:
                            self.__long_positions.pop(version)
                        price_bought = exec_b['price']
                        bought_time = exec_b['exec_time']
                        alert_b_ts = exec_b['alert_buy_ts']
                        buy_scanner = exec_b['scanner']
                        qty = exec_b['qty']
                        pct_chg = round(100 * (price_sold - price_bought) / price_bought, ndigits=4)
                        exec_time = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
                        execution_sell = dict(
                            event_type=EventType.TRADE,
                            ticker=ticker,
                            price=price_sold,
                            qty=qty,
                            side=side,
                            alerts_sell_ts=ts,
                            exec_time=exec_time,
                            scanner=source,
                        )
                        pnl = dict(
                            event_type=EventType.PNL,
                            ticker=ticker,
                            price_bought=price_bought,
                            price_sold=price_sold,
                            price_chg_pct=pct_chg,
                            qty=qty,
                            time_bought=bought_time,
                            time_sold=exec_time,
                            alert_buy_ts=alert_b_ts,
                            alert_sell_ts=ts,
                            buy_scanner=buy_scanner,
                            sell_scanner=source,
                            version=version,
                            pnl_type=PnlType.REALIZED.name
                        )
                        self.__pnl.setdefault(ticker, list()).append(pnl)
                        # self.__persister.insert_execution(execution_sell)
                        self.publish_event(execution_sell)
                        self.publish_event(pnl)
                    else:
                        logger.error(f'failed to execute sell for: {ticker}')
                else:
                    logger.info(f'No long positions for {ticker}, action [{side}] ignored for ticker: {ticker}')
        else:
            logger.warning('invalid event')

    def execute_buy(self, ticker):
        return self.get_price(ticker)

    def execute_sell(self, ticker):
        return self.get_price(ticker)

    def get_price(self, ticker):
        logger.info(f'getting price for {ticker}')
        try:
            quotes = self.__trader.get_quotes([ticker])
            if quotes:
                for t, q in quotes.items():
                    return q['lastPrice']
        except Exception:
            logger.exception(f'error getting price for {ticker}')
        return None

    def get_prices(self, tickers: list):
        logger.info(f'getting price for {tickers}')
        prices = {}
        try:
            quotes = self.__trader.get_quotes(tickers)
            if quotes:
                for t, q in quotes.items():
                    prices[t] = q['lastPrice']
        except Exception:
            logger.exception(f'error getting price for {tickers}')
        return prices

    def publish_event(self, event: dict):
        e_type = event['event_type']
        self.__event_handler.handle_event(event)
        if e_type == EventType.TRADE:
            payload = dict(event_type=EventType.POSITIONS, positions=self.__long_positions)
            self.__event_handler.handle_event(payload)

    def show_statistics(self):
        logger.info(f'current positions : {self.__long_positions}')
        logger.info('current PNLs summary ...')
        pnl_display = ""
        for (t, pnls) in self.__pnl.items():
            for pnl in pnls:
                pnl_display += str(pnl) + '\n'
        if pnl_display:
            logger.info(f'\n{pnl_display}')

    def gen_reports(self):
        logger.info('generating report ...')
        data = []
        for (t, pnls) in self.__pnl.items():
            data.extend(pnls)

        self.__reporter.gen_report(data)

    def eod_process(self):
        logger.info('eod process ...')
        if len(self.__long_positions) > 0:
            for (ver, positions) in self.__long_positions.items():
                tickers = list(positions.keys())
                eod_prices = self.get_prices(tickers)
                for t, p in eod_prices.items():
                    price_bought = positions[t]['price']
                    qty = positions[t]['qty']
                    alert_b_ts = positions[t]['alert_buy_ts']
                    pct_chg = round(100 * (p - price_bought) / price_bought, ndigits=4)
                    pnl = dict(
                        event_type=EventType.PNL,
                        ticker=t,
                        price_bought=price_bought,
                        price_sold=p,
                        price_chg_pct=pct_chg,
                        qty=qty,
                        time_bought=positions[t]['exec_time'],
                        time_sold='EOD',
                        alert_buy_ts=alert_b_ts,
                        alert_sell_ts="N/A",
                        buy_scanner=positions[t]['scanner'],
                        sell_scanner='EOD',
                        version=ver,
                        pnl_type=PnlType.UN_REALIZED.name
                    )
                    self.__pnl.setdefault(t, list()).append(pnl)

    def add_pnl(self, pnl: dict):
        self.__pnl.setdefault(pnl['ticker'], list()).append(pnl)

    def get_pnl(self):
        return self.__pnl

    def add_position(self, pos: dict):
        self.__long_positions.update(pos)

    def calc_qty(self, price_b):
        return self.__default_qty
        #max_buy_amt = min(self.__per_trade_amt_limit, self.__daily_trade_amt_limit-self.__total_amt)
        #return min(self.__default_qty, max_buy_amt//price_b)


if __name__ == "__main__":
    botManager = BotManager()
    botManager.start()
