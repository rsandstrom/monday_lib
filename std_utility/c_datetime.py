"""
DateTime class standard conversions for dates and times.

def unit_test():
    dt = DateTime()

    Now Supporting timestamps:

    start = 1644612284000
    dt = DateTime(start).as_datetime

    x = DateTime(start).as_timestamp

    Get the date only
    Example: logging.info(dt.date)

    Get the date 6 months ago
    Example: logging.info(dt.minus(months=6).date())

    Set the DateTime object to a datetime of 3 years from now
    Example: dt.datetime = dt.plus(years=3)

    Just use the DateTime class with default values
    logging.info(DateTime().minus(months=6))

    Built in parser will parse dates from many formats, in addition you can modify a DateTime to add or subtract
    years, months, days, hours, minutes, seconds + or -
    Example: logging.info(DateTime("2021-01-02", months=4).date)
    Example: logging.info(DateTime("2021-01-02 23:22:14.123456").date)
    Example: logging.info(DateTime("2021-01-02 23:22:14.123456", days=-24).date)

"""
import logging
from datetime import datetime

import pytz
from dateutil.relativedelta import relativedelta
from dateutil import parser


class DateTime:

    def __init__(self, dt=None, years: int = 0, months: int = 0, days: int = 0,
                 hours: int = 0, minutes: int = 0, seconds: int = 0, init=None, now=False, uct_now=False, tz=None):

        if init == 'now' or now is True:
            if tz is not None:
                dt = datetime.now(pytz.timezone(tz))
            else:
                dt = datetime.now()
        if init == 'uctnow' or uct_now is True:
            dt = datetime.utcnow()
        try:
            if isinstance(dt, int):
                dt = datetime.utcfromtimestamp(dt / 1000).strftime('%c')
                # dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dt / 1000))
        except Exception as ex:
            logging.warning(f"in DateTime unable to format unix timestamp, err = {ex}")
            pass

        dt = str(dt)

        if dt is None or len(dt) < 8:
            dt = '1970-01-01 00:00:00'
        self.dt = dt
        try:
            dt = str(dt)
            if isinstance(dt, str):
                self._datetime = parser.parse(dt)
            else:
                if dt is None:
                    self._datetime = datetime.now()
                else:
                    self._datetime = dt
            self._datetime = self.plus(years, months, days, hours, minutes, seconds)
        except parser.ParserError as ex:
            logging.info(f"Error caused by date with the following value [{dt}]")

    @property
    def is_datetime(self):
        return self._datetime.hour != 0 and self._datetime.min != 0 and self._datetime.second != 0
        # return self.to_time_str() == '00:00:00'

    @property
    def date(self):
        return self._datetime.date()

    @property
    def time(self):
        return self._datetime.time()

    @property
    def datetime(self):
        return self._datetime

    @property
    def as_date(self):
        return self._datetime.date()

    @property
    def as_time(self):
        return self._datetime.time()

    @property
    def as_datetime(self):
        return self._datetime

    @property
    def iso8601(self):
        return self._datetime.isoformat()

    @property  # returns timestamp in seconds
    def as_short_timestamp(self):
        return int((self._datetime - datetime(1970, 1, 1)).total_seconds())

    @property
    def as_timestamp(self):
        return int((self._datetime - datetime(1970, 1, 1)).total_seconds()) * 1000

    @property
    def db_format(self):
        return self.to_str()

    @property
    def utc_to_local(self):
        from dateutil import tz
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('America/New_York')

        utc = self._datetime

        utc = utc.replace(tzinfo=from_zone)

        eastern = utc.astimezone(to_zone)
        return eastern.strftime('%h %d %Y %I:%M %p')

    @property
    def db_data(self):
        if self.to_str() == '1970-01-01 00:00:00':
            return None
        else:
            return self.to_str()

    def from_date_str(self, dt_str: str = '1970-01-01'):
        return self._datetime.strptime(dt_str, "%Y-%m-%d")

    def to_str(self):
        return self._datetime.strftime('%Y-%m-%d %H:%M:%S')

    def us_date(self):
        return self._datetime.strftime('%m/%d/%Y')

    def to_date_str(self):
        return self._datetime.strftime('%Y-%m-%d')

    def to_time_str(self):
        return self._datetime.strftime('%H:%M:%S')

    def minus(self, years: int = 0, months: int = 0, days: int = 0,
              hours: int = 0, minutes: int = 0, seconds: int = 0) -> datetime:
        return self._datetime \
               - relativedelta(years=years) \
               - relativedelta(months=months) \
               - relativedelta(days=days) \
               - relativedelta(hours=hours) \
               - relativedelta(minutes=minutes) \
               - relativedelta(seconds=seconds)

    def plus(self, years: int = 0, months: int = 0, days: int = 0,
             hours: int = 0, minutes: int = 0, seconds: int = 0) -> datetime:
        return self._datetime \
               + relativedelta(years=years) \
               + relativedelta(months=months) \
               + relativedelta(days=days) \
               + relativedelta(hours=hours) \
               + relativedelta(minutes=minutes) \
               + relativedelta(seconds=seconds)

    def offset(self, years: int = 0, months: int = 0, days: int = 0,
               hours: int = 0, minutes: int = 0, seconds: int = 0) -> datetime:
        """ DateTime.set is used to adjust the date time of the object sot that it can be used
        with other properties such as DateTime.as_timestamp

        :param years:
        :param months:
        :param days:
        :param hours:
        :param minutes:
        :param seconds:
        :return:
        """
        self._datetime = self._datetime \
                         + relativedelta(years=years) \
                         + relativedelta(months=months) \
                         + relativedelta(days=days) \
                         + relativedelta(hours=hours) \
                         + relativedelta(minutes=minutes) \
                         + relativedelta(seconds=seconds)
        return self

    @staticmethod
    def parse(self, date_str):
        return parser.parse(date_str)


def unit_test():
    try:
        z = DateTime(now=True, tz='America/New_York').iso8601
        print(z)
        y = DateTime(now=True, tz='America/New_York').utc_to_local
        print(y)
        exit(1)


        start = 1644612284000
        dt = DateTime(start).as_datetime

        x = DateTime(start).as_timestamp

        assert start == x, 'timestamp does not match'

        r = DateTime('2022-03-22 14:55:45.287123-04:00')
        r = DateTime('2022-03-22T18:55:45.360251Z').db_format


        logging.info(x)

        dt = DateTime()
        logging.info(dt.date)
        logging.info(dt.minus(months=6).date())
        logging.info(dt.offset(months=6).as_date)

        dt._datetime = dt.plus(years=3)
        logging.info(dt.as_datetime)
        logging.info(dt.as_date)
        logging.info(DateTime().minus(months=6))

        logging.info(DateTime("2021-01-02", months=4).date)
        logging.info(DateTime("2021-01-02 23:22:14.123456").date)
        logging.info(DateTime("2021-01-02 23:22:14.123456", days=-24).to_date_str())
        logging.info(DateTime("2021-01-02 23:22:14.123456", days=-24).to_time_str())
    except Exception as ex:
        log_traceback(ex)


if __name__ == '__main__':
    init_logs('test', console=True, the_version='2.0.0', log_to_file=False)
    unit_test()
