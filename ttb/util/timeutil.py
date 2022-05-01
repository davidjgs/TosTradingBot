import datetime


def parse_time(time_hh_MM: str):
    t_hr = int(time_hh_MM.split(":")[0])
    t_min = int(time_hh_MM.split(":")[1])
    t_target = datetime.datetime.today().replace(hour=t_hr, minute=t_min,second=0,microsecond=0)
    return t_target



if __name__ == "__main__":

    t = parse_time("16:22")
    print(t)
