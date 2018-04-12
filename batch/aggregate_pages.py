#!/usr/bin/env python
from datetime import datetime
import logging

from pprint import pprint

from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession

from pyspark.sql.types import (StructType, StructField,
        StringType, IntegerType, TimestampType, ArrayType)


MONGO_HOST = 'localhost'
MONGO_DB_NAME = 'madkudu'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger('py4j').setLevel(logging.WARNING)


def get_mongo_uri(col_name):
    return 'mongodb://%s/%s.%s' % (
            MONGO_HOST, MONGO_DB_NAME, col_name)

def get_day(date):
    return datetime(date.year, date.month, date.day)

def get_distinct_viewed_pages(pages_df, days=7):
    """Get the distinct page names by user_id
    so that we can easily merge with real time data.
    """

    def create_combiner(x):
        return [x]

    def merge_value(acc, x):
        return acc + [x] if x not in acc else acc

    def merge_combiners(acc1, acc2):
        return list(set(acc1 + acc2))

    today = get_day(datetime.utcnow())
    begin = today + relativedelta(days=-days)
    return (pages_df
        .rdd
        .filter(lambda x: begin <= x['timestamp'] < today)
        .map(lambda x: (x['user_id'], x['name']))
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
        )
    )

def save(spark, stats_rdd):
    schema = StructType([
        StructField('user_id', StringType()),
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

def prepare_stats(user_id, distinct_viewed_pages):
    return {
        'user_id': user_id,
        'distinct_viewed_pages': distinct_viewed_pages,
        'timestamp': datetime.utcnow(),
    }

def process_stats(spark, pages_df):
    distinct_viewed_pages_rdd = get_distinct_viewed_pages(
            pages_df, days=7)
    stats_rdd = (distinct_viewed_pages_rdd
        .map(lambda (user_id, x): prepare_stats(user_id, x))
    )
    save(spark, stats_rdd)

def main():
    spark = (SparkSession
        .builder
        .appName('aggregate_page_views')
        .getOrCreate()
    )
    pages_df = (spark
        .read
        .format('com.mongodb.spark.sql.DefaultSource')
        .option('uri', get_mongo_uri('pages'))
        .load()
    )
    process_stats(spark, pages_df)


if __name__ == '__main__':
    main()
