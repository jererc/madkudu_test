#!/usr/bin/env python
from datetime import datetime
import calendar
import logging

from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
        StringType, IntegerType, TimestampType, ArrayType, MapType)
from pyspark.rdd import portable_hash


MONGO_HOST = 'localhost'
MONGO_DB_NAME = 'madkudu'
PAGE_TIME = 10  # maximum time spent per page in seconds

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

def get_distinct_viewed_pages(pages_rdd):
    """Get the distinct page names by user_id
    so that we can easily merge with real time data.
    """

    def create_combiner(x):
        return [x]

    def merge_value(acc, x):
        return acc if x in acc else acc + [x]

    def merge_combiners(acc1, acc2):
        return list(set(acc1 + acc2))

    return (pages_rdd
        .map(lambda x: (x['user_id'], x['name']))
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
        )
    )

def get_sessions_time(timestamps, page_time, sort_ts=False):

    def iter_times():
        sorted_timestamps = sorted(timestamps) \
                if sort_ts else timestamps
        for i, ts in enumerate(sorted_timestamps):
            try:
                yield min(page_time, sorted_timestamps[i + 1] - ts)
            except IndexError:
                yield page_time

    return sum(iter_times()) if timestamps else 0

def get_time_spent(pages_rdd, page_time=10):
    """Iterate over the sorted timestamps by user_id,
    and find sessions using page_time.
    """

    def create_combiner(x):
        return [x]

    def merge_value(acc, x):
        return acc if x in acc else acc + [x]

    def merge_combiners(acc1, acc2):
        return list(set(acc1 + acc2))

    partitions_count = 200  # we need more info on the input dataset (e.g.: maximum users count) in order to define this
    return (pages_rdd
        .map(lambda x: (x['user_id'], get_ts(x['timestamp'])))
        .map(lambda (user_id, ts): ((user_id, ts), ts))
        # Repartition by user_id hash and sort by the composite key (user_id, ts) within each partition
        .repartitionAndSortWithinPartitions(
                numPartitions=partitions_count,
                partitionFunc=lambda x: portable_hash(x[0]) % partitions_count,
                ascending=True)
        .map(lambda ((user_id, ts2), ts): (user_id, ts))
        # Group and deduplicate sorted timestamps by user_id (max: 7 * 24 * 3600 integers per user_id)
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
        )
        .mapValues(lambda x: get_sessions_time(x,
                page_time=page_time, sort_ts=False))
    )

def get_days_active(pages_rdd):

    def create_combiner(x):
        return [x]

    def merge_value(acc, x):
        return acc + [x]

    def merge_combiners(acc1, acc2):
        return acc1 + acc2

    return (pages_rdd
        # Divide timestamps by a day's number of seconds
        .map(lambda x: (x['user_id'], int(get_ts(x['timestamp']) / 86400)))
        .distinct()
        .mapValues(lambda x: datetime.utcfromtimestamp(x * 86400))
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
        )
    )

def get_viewed_pages_counts(pages_rdd):

    def create_combiner(x):
        return [x]

    def merge_value(acc, x):
        return acc + [x]

    def merge_combiners(acc1, acc2):
        return acc1 + acc2

    return (pages_rdd
        .map(lambda x: ((x['user_id'], x['name']), 1))
        .reduceByKey(lambda x, y: x + y)
        .map(lambda ((user_id, name), count): (user_id, (name, count)))
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
        )
        .mapValues(dict)
    )

def save(spark, stats_rdd):
    schema = StructType([
        StructField('user_id', StringType()),
        StructField('time_spent', IntegerType()),
        StructField('distinct_viewed_pages', ArrayType(StringType())),
        StructField('days_active', ArrayType(TimestampType())),
        StructField('viewed_pages_counts', MapType(
                StringType(), IntegerType())),
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
        'days_active': data[2],
        'viewed_pages_counts': data[3],
        'timestamp': datetime.utcnow(),
    }

def process_stats(spark, pages_df, last_days=7):
    today = get_day(datetime.utcnow())
    begin = today + relativedelta(days=-last_days)
    pages_rdd = (pages_df
        .rdd
        .filter(lambda x: begin <= x['timestamp'] < today)
    )

    distinct_viewed_pages_rdd = get_distinct_viewed_pages(pages_rdd)
    time_spent_rdd = get_time_spent(pages_rdd, page_time=PAGE_TIME)
    days_active_rdd = get_days_active(pages_rdd)
    viewed_pages_counts_rdd = get_viewed_pages_counts(pages_rdd)
    stats_rdd = (distinct_viewed_pages_rdd
        .join(time_spent_rdd)
        .join(days_active_rdd)
        .join(viewed_pages_counts_rdd)
        .mapValues(lambda (((d1, d2), d3), d4): (d1, d2, d3, d4))
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
