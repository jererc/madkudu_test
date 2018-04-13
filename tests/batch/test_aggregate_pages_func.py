#!/usr/bin/env python
import unittest
from operator import itemgetter
from datetime import datetime

from dateutil.relativedelta import relativedelta

from mock import Mock, patch

from pyspark.sql.types import (StructType, StructField,
        StringType, TimestampType)

from batch import aggregate_pages as module


class BaseSparkTestCase(unittest.TestCase):

    def setUp(self):
        self.spark = module.get_spark_session()
        self.sc = self.spark.sparkContext


class DistinctPagesTestCase(BaseSparkTestCase):

    def test_distinct_pages(self):
        today = module.get_day(datetime.utcnow())
        data = [
            {
                'user_id': '123',
                'name': 'page1',
                'timestamp': today + relativedelta(days=-3),
            },
            {
                'user_id': '123',
                'name': 'page2',
                'timestamp': today + relativedelta(days=-2),
            },
            {
                'user_id': '123',
                'name': 'page3',
                'timestamp': today + relativedelta(days=-1),
            },
        ]
        rdd = self.sc.parallelize(data)
        res = module.get_distinct_viewed_pages(rdd)

        rows = res.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], '123')
        self.assertEqual(sorted(rows[0][1]),
                ['page1', 'page2', 'page3'])


class TimeSpentSessionsTestCase(unittest.TestCase):

    def test_none(self):
        timestamps = []
        res = module.get_sessions_time(timestamps, page_time=30)
        self.assertEqual(res, 0)

    def test_single(self):
        timestamps = [0]
        res = module.get_sessions_time(timestamps, page_time=30)
        self.assertEqual(res, 30)

    def test1(self):
        timestamps = [100, 120, 200, 300]
        res = module.get_sessions_time(timestamps, page_time=30)
        self.assertEqual(res, 110)

    def test2(self):
        timestamps = [100, 101, 102, 103]
        res = module.get_sessions_time(timestamps, page_time=30)
        self.assertEqual(res, 33)

    def test_last_ts(self):
        timestamps = [100, 101, 102, 103, 103, 103]
        res = module.get_sessions_time(timestamps, page_time=30)
        self.assertEqual(res, 33)


class TimeSpentTestCase(BaseSparkTestCase):

    def test_transformation(self):
        today = module.get_day(datetime.utcnow())
        data = [
            {
                'user_id': '123',
                'name': 'page1',
                'timestamp': today + relativedelta(days=-3),
            },
            {
                'user_id': '123',
                'name': 'page2',
                'timestamp': today + relativedelta(days=-3, seconds=7),
            },
            {
                'user_id': '123',
                'name': 'page3',
                'timestamp': today + relativedelta(days=-1),
            },
            {
                'user_id': '456',
                'name': 'page1',
                'timestamp': today + relativedelta(days=-3),
            },
            {
                'user_id': '456',
                'name': 'page2',
                'timestamp': today + relativedelta(days=-3, seconds=2),
            },
            {
                'user_id': '456',
                'name': 'page3',
                'timestamp': today + relativedelta(days=-1),
            },
        ]
        rdd = self.sc.parallelize(data)
        schema = StructType([
            StructField('user_id', StringType()),
            StructField('name', StringType()),
            StructField('timestamp', TimestampType()),
        ])
        df = self.spark.createDataFrame(rdd, schema)

        with patch.object(module, 'save') as mock_save:
            module.process_stats(self.spark, df, last_days=7)

        saved_rdd = mock_save.call_args_list[0][0][1]
        rows = sorted(saved_rdd.collect(), key=itemgetter('user_id'))

        self.assertEqual(rows[0]['user_id'], '123')
        self.assertEqual(rows[0]['time_spent'], 27)
        self.assertEqual(rows[1]['user_id'], '456')
        self.assertEqual(rows[1]['time_spent'], 22)


if __name__ == '__main__':
    unittest.main()
