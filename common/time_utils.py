# -*- coding: UTF-8 -*-
"""
Description: MultiTool
@author: Men Luyao
@date: 2019/9/12
"""
import sys, json, re
from datetime import date, timedelta
import time, datetime, holidays
import pandas as pd
from common.Logger import logger


class TimeUtils:
    @staticmethod
    def validate_dt_format(time_str, format_str):
        if time_str is None or len(time_str) == 0:
            return None
        __format_re_list = [
            {'format': "%Y-%m-%d %H:%M:%S.%f", 're_rule': r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}\.\d{1,6})"},
            {'format': "%Y-%m-%d %H:%M:%S", 're_rule': r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})"},
            {'format': "%Y-%m-%d %H:%M", 're_rule': r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2})"},
            {'format': "%Y-%m-%d", 're_rule': r"(\d{4}-\d{1,2}-\d{1,2})"}
        ]
        __format_re_fork = {
            "%Y-%m-%d %H:%M:%S.%f": r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}\.\d{1,6})",
            "%Y-%m-%d %H:%M:%S": r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})",
            "%Y-%m-%d %H:%M": r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2})",
            "%Y-%m-%d": r"(\d{4}-\d{1,2}-\d{1,2})"
        }
        sorted(__format_re_fork.items(), key=lambda item: item[1])
        mat = re.search(__format_re_fork[format_str], time_str)
        if mat:
            return format_str
        else:
            for l in __format_re_list:
                mat_res = re.search(l['re_rule'], time_str)  # .groups()
                if mat_res:
                    return l['format']
        logger.warn('Wrong datetime string: {}, cannot match right format'.format(time_str))
        return None

    @staticmethod
    def string_toDatetime(time_str, format_str="%Y-%m-%d %H:%M:%S", is_check=True):
        try:
            if is_check and len(time_str) != 16:
                format_str = TimeUtils.validate_dt_format(time_str, format_str)
        except KeyError as e:
            if len(time_str) == 16:
                format_str = "%m/%d/%Y %H:%M"
        if len(time_str) == 16:
            format_str = "%m/%d/%Y %H:%M"
        if format_str is None:
            return None
        return datetime.datetime.strptime(time_str, format_str)

    @staticmethod
    def string_toTimestamp(time_str):
        ts = time.mktime(TimeUtils.string_toDatetime(time_str).timetuple())
        return int(round(ts * 1000))

    @staticmethod
    def datetime_toTimestamp(dt):
        ts = time.mktime(dt.timetuple())
        return int(round(ts))

    @staticmethod
    def timestamp_toString(ts, format_str='%Y-%m-%d %H:%M:%S'):
        if len(str(ts).split('.')[0]) != 10:
            logger.warn('Maybe wrong format of timestamp: {}, please check.'.format(ts))
        datetime_struct = datetime.datetime.fromtimestamp(ts)  # / 1000
        return datetime_struct.strftime(format_str)

    @staticmethod
    def datetime_toString(dt, format_str='%Y-%m-%d %H:%M:%S'):
        ts = time.mktime(dt.timetuple())
        return TimeUtils.timestamp_toString(ts, format_str=format_str)

    @staticmethod
    def get_now_as_str(format_str='%Y-%m-%d %H:%M:%S'):
        return datetime.datetime.now().strftime(format_str)

    @staticmethod
    def get_now_as_dt():
        return datetime.datetime.now()

    @staticmethod
    def get_yesterday_as_str(d_today=None, prev_format='%Y-%m-%d', cur_format='%Y-%m-%d'):
        # try:
        d_today = datetime.datetime.strptime(d_today, cur_format) if d_today and type(d_today) == str else date.today()
        return (d_today + timedelta(days=-1)).strftime(prev_format)
        # except ValueError as e:
        #     logger.error(e)

    @staticmethod
    def get_nextday_asString(cur_day, cur_format='%Y-%m-%d', next_format='%Y-%m-%d'):
        cur_day = datetime.datetime.strptime(cur_day, cur_format)
        return (cur_day + timedelta(days=1)).strftime(next_format)

    @staticmethod
    def get_last_month_asString(deli='-'):
        format_str = '%Y-%m-%d'
        today = TimeUtils.get_now_as_str(format_str=format_str)
        month = datetime.datetime.strptime(today, format_str).month
        if month == 1:
            year = datetime.datetime.strptime(today, format_str).year
            year = year - 1
            month = 12
        else:
            year = datetime.datetime.strptime(today, format_str).year
            month = month - 1
        return '{}{}{}'.format(str(year), deli, str(month))

    @staticmethod
    def get_time_until_day_by_str(time_str, input_format_str="%Y-%m-%d %H:%M:%S.%f"):
        input_format_str = TimeUtils.validate_dt_format(time_str, input_format_str)
        if input_format_str is None:
            return None
        ts = TimeUtils.string_toDatetime(time_str, input_format_str)
        return ts.strftime('%Y-%m-%d')

    @staticmethod
    def get_hour_by_str(time_str, input_format_str="%Y-%m-%d %H:%M:%S.%f"):
        ts = TimeUtils.string_toDatetime(time_str, input_format_str)
        return ts.strftime('%H')

    @staticmethod
    def get_date_by_str(time_str, input_format_str="%Y-%m-%d %H:%M:%S.%f", is_check=True):
        if time_str is None or len(time_str) == 0:
            return None
        try:
            # format_str = TimeCommon.validate_dt_format(time_str, input_format_str)
            ts = TimeUtils.string_toDatetime(time_str, input_format_str, is_check=is_check)
            return ts.strftime('%Y-%m-%d')
        except AttributeError:
            if '/' in time_str:
                ts = TimeUtils.string_toDatetime(time_str, "%m/%d/%Y %H:%M", is_check=is_check)
                return ts.strftime('%Y-%m-%d')
            logger.error('{}, time_str is {}'.format(sys.exc_info()[1], time_str))
            return None

    @staticmethod
    def get_month_by_str(time_str, input_format_str="%Y-%m-%d %H:%M:%S.%f"):
        try:
            # if len(time_str) == 16:
            #     input_format_str = "%Y-%m-%d %H:%M"
            ts = TimeUtils.string_toDatetime(time_str, input_format_str)
            return ts.strftime('%Y-%m')
        except AttributeError:
            logger.error('{}, time_str is {}'.format(sys.exc_info()[1], time_str))
            return None

    @staticmethod
    def compare_time(date1, date2):
        strftime1 = datetime.datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
        strftime2 = datetime.datetime.strptime(date2, "%Y-%m-%d %H:%M:%S")
        if strftime1 > strftime2:
            return 1
        elif strftime1 < strftime2:
            return -1
        else:
            return 0

    @staticmethod
    def compare_date_by_dt(date1, date2, format_str="%Y-%m-%d %H:%M:%S"):
        if not date1 or not date2 or len(date1) == 0 or len(date2) == 0:
            return None
        date1 = TimeUtils.get_date_by_str(date1, input_format_str=format_str)
        date2 = TimeUtils.get_date_by_str(date2, input_format_str=format_str)
        strftime1 = datetime.datetime.strptime(date1, "%Y-%m-%d")
        strftime2 = datetime.datetime.strptime(date2, "%Y-%m-%d")
        if strftime1 > strftime2:
            return 1
        elif strftime1 < strftime2:
            return -1
        else:
            return 0

    @staticmethod
    def get_date_from_range(begin_date, end_date, format_str='%Y-%m-%d', closed_interval=True, mode='D', interval=1):
        dates = list()
        if not begin_date or len(begin_date) == 0 or not end_date or len(end_date) == 0 or begin_date >= end_date:
            return dates

        mode = mode.upper()
        __next_dt_forks = {
            'H': lambda cur_dt, itrv: cur_dt + datetime.timedelta(hours=itrv),
            'D': lambda cur_dt, itrv: cur_dt + datetime.timedelta(days=itrv),
            'W': lambda cur_dt, itrv: cur_dt + datetime.timedelta(weeks=itrv),
            'M': lambda cur_dt, itrv: cur_dt.replace(month=cur_dt.month + itrv) if cur_dt.month < 12 else cur_dt.replace(year=cur_dt.year+1, month=1),
            'Y': lambda cur_dt, itrv: cur_dt + datetime.timedelta(days=itrv * 365)  # TODO Y need 4bit, not 8bit with days
        }
        dt = datetime.datetime.strptime(begin_date, format_str)
        bg_date = begin_date
        while bg_date <= end_date:
            dates.append(bg_date)
            dt = __next_dt_forks[mode](dt, interval)
            bg_date = dt.strftime(format_str)
        if not closed_interval:
            if dates[0] == begin_date:
                dates.pop(0)
            if dates[len(dates) - 1] == end_date:
                dates.pop(len(dates) - 1)
        return dates

    @staticmethod
    def in_range_until_today(day1, diff_range, day2=None):
        if not day2:
            day2 = TimeUtils.get_now_as_str()
        day1 = TimeUtils.string_toDatetime(TimeUtils.get_date_by_str(day1), format_str="%Y-%m-%d")
        day2 = TimeUtils.string_toDatetime(TimeUtils.get_date_by_str(day2), format_str="%Y-%m-%d")
        diff = (day2 - day1).days
        if (diff >= diff_range[0]) & (diff <= diff_range[1]):
            return True
        else:
            return False

    @staticmethod
    def adjust_months(ori_dt, diff_months):
        """
        Calculation method:
        Year = 1. if [ current month of ori_dt + diff_months ] != multiple 12, current year of ori_dt + [ current month of ori_dt + diff_months / 12 ]
               2. if [ current month of ori_dt + diff_months ] = multiple 12, current year of ori_dt + [ current month of ori_dt + diff_months / 12 ] - 1
        Month = 1. if [ current month of ori_dt + diff_months ] != multiple 12, ( current month of ori_dt + 12 + diff_months ) % 12
                2. if [ current month of ori_dt + diff_months ] = multiple 12, 12

        :param ori_dt: origin datetime
        :param diff_months: difference of months, positive or negative number
        :return: datetime after calculating
        """
        new_month = ori_dt.month + diff_months
        cur_month = new_month % 12
        cur_year = ori_dt.year + new_month // 12 - (0 if cur_month else 1)
        new_dt = ori_dt.replace(year=cur_year, month=cur_month if cur_month else 12)
        return new_dt

    @staticmethod
    def is_usa_holiday(year, month, day):
        us_holidays = holidays.US()
        return '{}-{}-{}'.format(month if month > 9 else '0{}'.format(month),
                                 day if day > 9 else '0{}'.format(day), year) in us_holidays

    @staticmethod
    def is_business_day(date):
        return bool(len(pd.bdate_range(date, date)))


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


if __name__ == '__main__':
    # test = TimeUtils.is_usa_holiday(2021, 1, 1)
    test = TimeUtils.is_business_day('2021-1-1')
    # time_str = '7/1/2021 7:14'
    # test = TimeUtils.string_toDatetime(time_str, format_str="%m/%d/%Y %H:%M", is_check=False)
    # # string_toDatetime(time_str, format_str="%Y-%m-%d %H:%M:%S", is_check=True)
    print(test)



