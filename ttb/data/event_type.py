from enum import Enum


class EventType(Enum):
    EMAIL_ALERT = 1
    TRADE = 2
    PNL = 3
    POSITIONS = 4
    RPA_ALERT = 5

