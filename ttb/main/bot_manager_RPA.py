import time

from ttb.cfg.config import Config
from ttb.control.trading_control import TradeControl
from ttb.data.event_type import EventType
from ttb.data.pnl_type import PnlType
from ttb.db.db_persist import DBPersister
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
        self.cob_date = self.__conf.date_today
        self.default_acct = self.__conf.default_acct
        self.__event_q = Queue()
        self.__ticker_q = Queue()
        self.__hist_price_q = Queue()
        self.__default_qty = self.__conf.trade_default_qty
        self.__per_trade_amt_limit = self.__conf.per_trade_amt_limit
        self.__daily_trade_amt_limit = self.__conf.daily_trade_amt_limit
        self.__total_amt = 0
        self.trading_end_time = timeutil.parse_time(self.__conf.trade_end_time)
        self.long_positions = {}
        self.__executions = {}
        self.__pnl = {}
        self.__persister = DBPersister()
        self.__alert_reader = AlertReader(self.__conf, self.__event_q, self.__persister)
        self.__trader = TosTrader(self.__conf)
        self.__event_handler = ToFileEventHandler(self.__conf)
        self.__reporter = PnlReporter(config=self.__conf)
        self.trade_control = TradeControl(self.__conf)

    def start(self):

        logger.info('Starting BotManager ...')

        thread = threading.Thread(target=self.__alert_reader.start)
        thread.start()
        ##threading.Thread(target=self.__priceAnalyzer.start).start()
        self.work()

    def work(self):
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
                execution = None
                pnl = None
                exec_time = ''.join(datetime.datetime.now(tz=datetime.datetime.now().astimezone().tzinfo).isoformat(
                    timespec='milliseconds').rsplit(':', 1))
                if side == 'BUY' and (version not in self.long_positions or ticker not in self.long_positions[version]):
                    price_b = self.execute_buy(ticker)
                    if price_b:
                        buy_qty = self.calc_qty(price_b)
                        if buy_qty:
                            self.long_positions.setdefault(version, dict())[ticker] = {'price': float(price_b),
                                                                                         'qty': buy_qty,
                                                                                         'exec_time': exec_time,
                                                                                         'alert_buy_ts': ts,
                                                                                         'strategy': source}  # (float(price_b), exec_time)
                            # self.__priceAnalyzer.add_price(ticker, price_b, dt.strftime('%H%M%S'))
                            execution = dict(
                                event_type=EventType.TRADE,
                                ticker=ticker,
                                price=price_b,
                                qty=buy_qty,
                                side=side,
                                exec_time=exec_time,
                                alert_buy_ts=ts,
                                strategy=source,
                            )
                            self.__total_amt += price_b*buy_qty
                            self.__ticker_q.put(ticker)
                        else:
                            logger.info(f"# of shares < 1, skipped buy for : {ticker}")
                    else:
                        logger.error(f'failed to execute buy for : {ticker}')
                elif side == 'SELL' and version in self.long_positions and ticker in self.long_positions[version]:
                    price_s = self.execute_sell(ticker)
                    if price_s:
                        price_sold = float(price_s)
                        exec_b = self.long_positions[version].pop(ticker)
                        if len(self.long_positions[version]) == 0:
                            self.long_positions.pop(version)
                        price_bought = exec_b['price']
                        bought_time = exec_b['exec_time']
                        alert_b_ts = exec_b['alert_buy_ts']
                        buy_strategy = exec_b['strategy']
                        qty = exec_b['qty']
                        pct_chg = round(100 * (price_sold - price_bought) / price_bought, ndigits=4)
                        execution = dict(
                            event_type=EventType.TRADE,
                            ticker=ticker,
                            price=price_sold,
                            qty=qty,
                            side=side,
                            alerts_sell_ts=ts,
                            exec_time=exec_time,
                            strategy=source,
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
                            buy_strategy=buy_strategy,
                            sell_strategy=source,
                            version=version,
                            pnl_type=PnlType.REALIZED.name
                        )
                        self.__pnl.setdefault(ticker, list()).append(pnl)
                    else:
                        logger.error(f'failed to execute sell for: {ticker}')
                else:
                    logger.info(f'No long positions for {ticker}, action [{side}] ignored for ticker: {ticker}')

                if execution:
                    self.publish_event(execution)
                    self.__persister.insert_execution(self.enrich(execution))
                if pnl:
                    self.publish_event(pnl)
                    self.__persister.insert_pnl(self.enrich(pnl))
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
            ## also update positions
            payload = dict(event_type=EventType.POSITIONS, positions=self.long_positions)
            self.__event_handler.handle_event(payload)

    def show_statistics(self):
        logger.info(f'current positions : {self.long_positions}')
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
        eod_price_list = {'event_type': EventType.EOD_PRICE, "eod_prices": {}}
        tickers = set()
        for (ver, positions) in self.long_positions.items():
            tickers.update(positions.keys())
        eod_prices = self.get_prices(list(tickers))
        for (ver, positions) in self.long_positions.items():
            for t, pos in positions.items():
                price_bought = pos['price']
                p = eod_prices.get(t) or price_bought
                qty = pos['qty']
                alert_b_ts = pos['alert_buy_ts']
                pct_chg = round(100 * (p - price_bought) / price_bought, ndigits=4)
                pnl = dict(
                    event_type=EventType.PNL,
                    ticker=t,
                    price_bought=price_bought,
                    price_sold=p,
                    price_chg_pct=pct_chg,
                    qty=qty,
                    time_bought=pos['exec_time'],
                    time_sold='EOD',
                    alert_buy_ts=alert_b_ts,
                    alert_sell_ts="N/A",
                    buy_strategy=pos['strategy'],
                    sell_strategy='EOD',
                    version=ver,
                    pnl_type=PnlType.UN_REALIZED.name
                )
                self.__pnl.setdefault(t, list()).append(pnl)
                eod_price_list['eod_prices'][t] = p
        if tickers:
            self.publish_event(eod_price_list)
            self.__persister.insert_eod_prices(self.enrich(eod_price_list))

    def add_pnl(self, pnl: dict):
        self.__pnl.setdefault(pnl['ticker'], list()).append(pnl)

    def get_pnl(self):
        return self.__pnl

    def add_position(self, pos: dict):
        self.long_positions.update(pos)

    def calc_qty(self, price_b):
        return self.__default_qty
        #max_buy_amt = min(self.__per_trade_amt_limit, self.__daily_trade_amt_limit-self.__total_amt)
        #return min(self.__default_qty, max_buy_amt//price_b)

    def enrich(self, data: dict):
        data['cob_date'] = self.cob_date
        data['account'] = self.default_acct

        return data

'''
def test_eod_price():
    botManager.long_positions = {}
    botManager.long_positions['V5.4'] = {}
    botManager.long_positions['V5.4']['LIT'] = {}
    botManager.long_positions['V5.4']['LIT']['price'] = 116.0
    botManager.long_positions['V5.4']['LIT']['qty'] = 100
    botManager.long_positions['V5.4']['LIT']['version'] = 'V5.4'
    botManager.long_positions['V5.4']['LIT']['alert_buy_ts'] = '2022/05/13-09:57:14'
    botManager.long_positions['V5.4']['LIT']['exec_time'] = '2022/05/13-09:57:14'
    botManager.long_positions['V5.4']['LIT']['strategy'] = '#B4#V5.4#'

    botManager.long_positions['V5.4']['LCID'] = {}
    botManager.long_positions['V5.4']['LCID']['price'] = 17.1
    botManager.long_positions['V5.4']['LCID']['qty'] = 100
    botManager.long_positions['V5.4']['LCID']['version'] = 'V5.4'
    botManager.long_positions['V5.4']['LCID']['alert_buy_ts'] = '2022/05/13-09:57:14'
    botManager.long_positions['V5.4']['LCID']['exec_time'] = '2022/05/13-09:57:14'
    botManager.long_positions['V5.4']['LCID']['strategy'] = '#B4#V5.4#'
    botManager.eod_process()
'''

if __name__ == "__main__":
    botManager = BotManager()
    botManager.start()

