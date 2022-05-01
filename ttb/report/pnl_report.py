import os
from datetime import datetime

import pandas as pd
import openpyxl

d_fmt = "%Y%m%d"

from ttb.cfg.config import Config

PNL_RPT_HEADER = ['ticker', 'price_bought', 'price_sold', 'qty', 'price_chg_%', 'PNL', 'transc_amt', 'time_bought', 'time_sold','alert_buy_ts','alert_sell_ts',  'buy_scanner', 'sell_scanner', 'pnl_type', 'pnl_count']
class PnlReporter:
    def __init__(self, config: Config = None):
        self.conf = config or Config()
        self.__today = self.conf.date_today
        self.output_dir = f'{self.conf.report_out_dir}{os.sep}{self.__today}'
        self.pnl_file_name = "pnl_report"

    def gen_report(self, data: list):
        rpt_data = transform(data)
        report_time = datetime.now().strftime("%Y%m%d%H%M")
        out_file = f'{self.output_dir}/{self.pnl_file_name}_{report_time}.xlsx'
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        with pd.ExcelWriter(out_file) as writer:
            for version, v_data in rpt_data.items():
                df = pd.DataFrame(v_data, columns=PNL_RPT_HEADER)
                df.to_excel(writer, sheet_name=f'{version}')
                ##df2.to_excel(writer, sheet_name='sheet2')


def transform(data: list):
    rs = {}
    for r in data:
        price_b = float(r['price_bought'])
        price_s = float(r['price_sold'])
        qty = float(r['qty'])
        version = r.get('version') or 'UNKOWN'
        rs.setdefault(version, list()).append([
            r['ticker'],
            price_b,
            price_s,
            qty,
            r['price_chg_pct'],
            (price_s - price_b) * qty,
            price_b * qty,
            r['time_bought'],
            r['time_sold'],
            r['alert_buy_ts'],
            r['alert_sell_ts'],
            r['buy_scanner'],
            r['sell_scanner'],
            r['pnl_type'],
            1 if r['price_chg_pct'] > 0 else -1 if r['price_chg_pct'] < 0 else 0
        ])
    return rs
