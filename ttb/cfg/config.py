import os

from ttb.util.singleton import Singleton
from datetime import datetime

fmt_Ymd = "%Y%m%d"
CLIENT_ID = '686FJCQ81PSX5HEOCPZYQ0JM8RRUR0BQ'
CALL_BACK_URL = 'https://localhost'
CRED_PATH = '../auth/cred.json'


class Config(metaclass=Singleton):
    def __init__(self, config_file=None):
        self.config_file = config_file

    @property
    def date_today(self):
        return datetime.now().strftime(fmt_Ymd)

    def mongo_url(self):
        return 'localhost:27017'

    def mongo_db(self):
        return 'trading'

    @property
    def td_client_id(self):
        return CLIENT_ID

    @property
    def td_call_back_url(self):
        return CALL_BACK_URL

    @property
    def td_credentials_path(self):
        return CRED_PATH

    @property
    def mail_alert_sender(self):
        return "alerts@thinkorswim.com"

    @property
    def mail_token_path(self):
        return "../auth/token.json"

    @property
    def mail_cred_path(self):
        return "../auth/credentials.json"

    ## gmail alert pull interval (seconds)
    @property
    def mail_pull_interval_seconds(self):
        return 30

    ## rpa alert pull interval (seconds)
    @property
    def alert_pull_interval_seconds(self):
        return 10

    @property
    def watchlist_pull_interval_seconds(self):
        return 20

    ## trading day start time
    @property
    def trade_start_time(self):
        return "9:30"

    ## trading day end time
    @property
    def trade_end_time(self):
        return "16:00"

    ## BUY start time
    @property
    def buy_start_time(self):
        return "9:30"

    ## BUY end time
    @property
    def buy_end_time(self):
        return "15:00"

    @property
    def report_out_dir(self):
        return self.gdrive_path

    @property
    def gdrive_token_path(self):
        return "../auth/token.json"

    @property
    def gdrive_cred_path(self):
        return "../auth/credentials.json"

    @property
    def gdrive_path(self):
        return "G:\\My Drive\\TradingBot"
        #return "C:\\tosBot\\reports"

    @property
    def journal_dir(self):
        return self.gdrive_path

    @property
    def trades_journal(self):
        return "trades"

    @property
    def events_journal(self):
        return "events"

    @property
    def pnl_journal(self):
        return "pnl"

    @property
    def open_positions_journal(self):
        return "open_positions"

    @property
    def long_watch_list_name(self):
        return 'LONG_POS'

    @property
    def price_poll_freq(self):
        return 180

    #### alert related properties
    @property
    def alert_src_group(self):
        return ["V5.4", "V5.5"]

    @property
    def alert_timeout_seconds(self):
        return 300

    @property
    def alerts_buy_min_votes(self):
        return len(self.alert_src_group)

    @property
    def alerts_sell_min_votes(self):
        return 1

    @property
    def buy_watchlist(self):
        return "LIVE-BSKT"

    @property
    def sell_watchlist(self):
        return "LIVE-BSKT"

    @property
    def wl_account(self):
        return "252191256"

    ## default by quantity
    @property
    def trade_default_qty(self):
        return 100

    ## per trade limit ($ amt)
    @property
    def per_trade_amt_limit(self):
        return 20000

    ## Daily Trading limit ($ amt)
    @property
    def daily_trade_amt_limit(self):
        return 300000

    @property
    def alert_dir(self):
        return f'{self.gdrive_path}{os.sep}{self.date_today}'
        #return f'C:/tosBot{os.sep}{self.date_today}'

    @property
    def alert_file_path(self):
        return f'{self.alert_dir}{os.sep}scanner_alerts_{self.date_today}.json'
