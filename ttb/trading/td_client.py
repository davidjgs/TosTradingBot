import time

from td.client import TDClient

from ttb.cfg.config import Config
from ttb.trading import watchlist_utils


class TosTrader:
    def __init__(self, config=None):
        self.conf = config or Config()
        self.long_pos_wl_name = self.conf.long_watch_list_name
        self.__wl_id = None
        self.__wl_inst_cache = None
        self.tdClient = TDClient(
            client_id=self.conf.td_client_id,
            redirect_uri=self.conf.td_call_back_url,
            credentials_path=self.conf.td_credentials_path
        )
        # self.tdSession.login()

    def get_quotes(self, instruments: list):
        self.tdClient.login()
        return self.tdClient.get_quotes(instruments=instruments)

    def get_price_history(self, symbol, ):
        return self.tdClient.get_price_history(symbol, period_type="day")

    def get_instrument(self, cusip: str):
        return self.tdClient.get_instruments(cusip)

    def get_account_id(self, type='CASH'):
        accounts = self.tdClient.get_accounts()
        for acct in accounts:
            if 'securitiesAccount' in acct and acct['securitiesAccount']['type'] == type:
                return acct['securitiesAccount']['accountId']
        return None

    def get_wlid_by_wlname(self, wl_name, account):
        watch_lists = self.tdClient.get_watchlist_accounts(account=account)
        for watch_list in watch_lists:
            if watch_list['name'] == wl_name:
                return watch_list['watchlistId']
        return None

    def get_watchlist_instruments(self, wl_name, account):
        instruemnts = []
        watch_lists = self.tdClient.get_watchlist_accounts(account=account)
        for watch_list in watch_lists:
            if watch_list['name'] == wl_name:
                watch_list_items = watch_list['watchlistItems']
                for item in watch_list_items:
                    instruemnts.append(item['instrument']['symbol'])
        return instruemnts

    def create_or_update_watchlist(self, symbols, wl_name=None, acct=None):
        wl_name = wl_name or self.long_pos_wl_name
        acct = acct or self.get_account_id()
        print(f'acct = ' + acct)
        watchlst_id = self.__wl_id or self.get_wlid_by_wlname(wl_name=wl_name, account=acct)
        if not watchlst_id:
            self.__create_new_watchlist(symbols, wl_name, acct)
        else:
            self.__wl_id = watchlst_id
            self.__update_watchlist(symbols, wl_name, watchlst_id, acct)

    def __create_new_watchlist(self, symbs, wl_name, acct):
        items = watchlist_utils.create_watch_list_items(symbs)
        self.tdClient.create_watchlist(account='252191256', name=wl_name, watchlistItems=items)
        self.__wl_inst_cache = set(symbs)

    def __update_watchlist(self, symbols, wl_name, wl_id, acct):
        existing_symbols = self.__wl_inst_cache or self._get_symbols_in_wl(watchlst_id=wl_id, acct=acct)
        all_symbols = set(existing_symbols.extend(symbols))
        self.__wl_inst_cache = all_symbols
        items = watchlist_utils.create_watch_list_items(symbols)
        rs = self.tdClient.create_watchlist(account=acct, name=wl_name, watchlistItems=items)

    def _get_symbols_in_wl(self, watchlst_id, acct):
        wl = self.tdClient.get_watchlist(account=acct, watchlist_id=watchlst_id)
        rs = set()
        if wl:
            wl_items = wl['watchlistItems']
            for item in wl_items:
                rs.add(item['instrument']['symbol'])
        return rs


if __name__ == "__main__":
    td_trader = TosTrader()

    ##quotes = td_trader.get_quotes(['DDOG', 'SQ'])

    ##print(f'Quotes ==> {quotes}', end='\n')
    '''
    inst = td_trader.get_instrument('852234103')
    print(f'Instrument ==> {inst}', end='\n')

    accountId = td_trader.get_account_id()
    print(f'Account_id ==> {accountId}', end='\n')
  
    for i in range(1, 30):
        instruments = td_trader.get_watchlist_instruments('LIVE-BSKT', account='252191256')
        print(f'{i}. instruments[{len(instruments)}] ==> {instruments}', end='\n')
        time.sleep(20)

  
    symbols = td_trader._get_symbols_in_wl(watchlst_id='1773132247', acct='252191256')
    print(f'symbols ==> {symbols}', end='\n')
    '''
    wlist = td_trader.create_or_update_watchlist(wl_name='TEST1', symbols=['FCEL'], acct='252191256')
    print(f'wList ==> {wlist}', end='\n')

