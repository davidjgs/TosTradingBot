from datetime import datetime

fmt_Ymd = "%Y%m%d"


def date_today():
    return datetime.now().strftime(fmt_Ymd)
