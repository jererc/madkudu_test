#!/usr/bin/env python
from datetime import datetime
import calendar
import logging

from pprint import pprint

from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
        StringType, IntegerType, TimestampType, ArrayType)


MONGO_HOST = 'localhost'
MONGO_DB_NAME = 'madkudu'
PAGE_TIME = 10  # session duration in seconds

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('py4j').setLevel(logging.WARNING)


def get_mongo_uri(col_name):
    return 'mongodb://%s/%s.%s' % (
            MONGO_HOST, MONGO_DB_NAME, col_name)

def get_day(date):
    return datetime(date.year, date.month, date.day)

def get_ts(date):
    return calendar.timegm(date.timetuple())

def get_distinct_viewed_pages(users_rdd):
    """Get the distinct page names by user_id
    so that we can easily merge with real time data.
    """

    def create_combiner(x):
        return [x]

    def merge_value(acc, x):
        return acc if x in acc else acc + [x]

    def merge_combiners(acc1, acc2):
        return list(set(acc1 + acc2))

    return (users_rdd
        .map(lambda x: (x['user_id'], x['name']))
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
        )
    )

def get_sessions_time(timestamps, page_time):

    def iter_times():
        sorted_timestamps = sorted(timestamps)
        last_index = len(sorted_timestamps) - 1
        begin = sorted_timestamps[0]
        end = begin + page_time

        for index, ts in enumerate(sorted_timestamps):
            if ts < end:
                end = ts + page_time
            else:
                yield end - begin
                begin = ts
                end = ts + page_time

            if index == last_index:
                yield end - begin

    if not timestamps:
        return 0
    return sum(iter_times())

def get_time_spent(users_rdd, page_time=30):
    """Iterate over the sorted timestamps by user_id,
    and find sessions using page_time seconds.
    """

    def create_combiner(x):
        return [x]

    def merge_value(acc, x):
        return acc + [x]

    def merge_combiners(acc1, acc2):
        return acc1 + acc2

    return (users_rdd
        .map(lambda x: (x['user_id'], get_ts(x['timestamp'])))
        # WARNING: not good, won't scale and we could get loads of events per user_id...
        # We could sort within user_ids partitions using repartitionAndSortWithinPartitions()
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
        )
        .mapValues(lambda x: get_sessions_time(x, page_time=page_time))
    )

def save(spark, stats_rdd):
    schema = StructType([
        StructField('user_id', StringType()),
        StructField('time_spent', IntegerType()),
        StructField('distinct_viewed_pages', ArrayType(StringType())),
        StructField('timestamp', TimestampType()),
    ])
    df = spark.createDataFrame(stats_rdd, schema)
    (df
        .write
        .format('com.mongodb.spark.sql.DefaultSource')
        .option('uri', get_mongo_uri('aggregated_pages'))
        .mode('append')
        .save()
    )

def prepare_stats(user_id, data):
    return {
        'user_id': user_id,
        'distinct_viewed_pages': data[0],
        'time_spent': data[1],
        'timestamp': datetime.utcnow(),
    }

def process_stats(spark, pages_df, last_days=7):
    today = get_day(datetime.utcnow())
    begin = today + relativedelta(days=-last_days)
    users_rdd = (pages_df
        .rdd
        .filter(lambda x: begin <= x['timestamp'] < today)
    )

    distinct_viewed_pages_rdd = get_distinct_viewed_pages(users_rdd)
    time_spent_rdd = get_time_spent(users_rdd, page_time=PAGE_TIME)
    stats_rdd = (distinct_viewed_pages_rdd
        .leftOuterJoin(time_spent_rdd)
        .map(lambda (user_id, x): prepare_stats(user_id, x))
    )

    save(spark, stats_rdd)

def get_spark_session():
    return (SparkSession
        .builder
        .appName('aggregate_page_views')
        .getOrCreate()
    )

def main():
    spark = get_spark_session()
    pages_df = (spark
        .read
        .format('com.mongodb.spark.sql.DefaultSource')
        .option('uri', get_mongo_uri('pages'))
        .load()
    )
    process_stats(spark, pages_df, last_days=7)


if __name__ == '__main__':
    main()
