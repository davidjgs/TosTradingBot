import datetime
import json
import os

from ttb.cfg.config import Config
from ttb.data.event_type import EventType
from ttb.event import EventHandler
import logging

logger = logging.getLogger("ToFileEventHandler")

d_fmt = "%Y%m%d"


class ToFileEventHandler(EventHandler):
    def __init__(self, conf: Config):
        self.__trade_file = conf.trades_journal
        self.__event_file = conf.events_journal
        self.__pnl_file = conf.pnl_journal
        self.__positions_file = conf.open_positions_journal
        self.__eod_price_file = conf.eod_prices_file
        self.__today = conf.date_today
        self.__target_dir = f'{conf.journal_dir}{os.sep}{self.__today}'
        if not os.path.exists(self.__target_dir):
            os.makedirs(self.__target_dir)

    def handle_event(self, event: dict):
        eventType = event['event_type']
        event['event_type'] = eventType.name
        event_destination = self.__get_event_destination(eventType)
        data_json = json.dumps(event)
        logger.info(f'publishing event: {data_json}')
        m = 'w' if eventType == EventType.POSITIONS else 'a'
        with open(event_destination, mode=m) as fw:
            fw.write(f'{data_json}\n')

    def __get_event_destination(self, eventType: EventType):
        chosen = None
        if eventType == EventType.EMAIL_ALERT or eventType == EventType.RPA_ALERT:
            chosen = self.__event_file
        elif eventType == EventType.TRADE:
            chosen = self.__trade_file
        elif eventType == EventType.PNL:
            chosen = self.__pnl_file
        elif eventType == EventType.POSITIONS:
            chosen = self.__positions_file
        elif eventType == EventType.EOD_PRICE:
            chosen = self.__eod_price_file
        return f"{self.__target_dir}{os.sep}{chosen}_{self.__today}.json" if chosen else None
