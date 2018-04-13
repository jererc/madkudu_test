from datetime import datetime
import calendar
import logging

from dateutil import parser as date_parser

from pymongo import MongoClient

import settings


logger = logging.getLogger(__name__)

_mongo_client = None


def _get_mongo_client():
    global _mongo_client
    if _mongo_client is None:
        logger.debug('connecting to mongod on %s',
                settings.MONGO_HOST)
        _mongo_client = MongoClient(settings.MONGO_HOST)
    return _mongo_client

def get_mongo_collection(col_name):
    return _get_mongo_client()[settings.MONGO_DB_NAME][col_name]

def parse_ts(ts):
    try:
        return date_parser.parse(ts)
    except ValueError, e:
        raise Exception('failed to parse timestamp "%s": %s' % (
                ts, str(e)))

def get_day(date):
    return datetime(date.year, date.month, date.day)

def get_ts(date):
    return calendar.timegm(date.timetuple())
