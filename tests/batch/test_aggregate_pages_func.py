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
        res = module.get_sessions_time(timestamps, page_time=10)
        self.assertEqual(res, 0)

    def test_single(self):
        timestamps = [100]
        res = module.get_sessions_time(timestamps, page_time=10)
        self.assertEqual(res, 10)

    def test1(self):
        timestamps = [100, 104, 200, 300]
        res = module.get_sessions_time(timestamps, page_time=10)
        self.assertEqual(res, 34)

    def test2(self):
        timestamps = [100, 101, 102, 103]
        res = module.get_sessions_time(timestamps, page_time=10)
        self.assertEqual(res, 13)

    def test_last_ts(self):
        timestamps = [100, 101, 102, 103, 103, 103]
        res = module.get_sessions_time(timestamps, page_time=10)
        self.assertEqual(res, 13)


class TimeSpentTestCase(BaseSparkTestCase):

    def test_sort(self):
        today = module.get_day(datetime.utcnow())
        data = [
            {'user_id': '000', 'timestamp': today + relativedelta(days=-3, seconds=6, microseconds=789)},
            {'user_id': '000', 'timestamp': today + relativedelta(days=-3, microseconds=333)},
            {'user_id': '000', 'timestamp': today + relativedelta(days=-3, seconds=2, microseconds=111)},

            {'user_id': '001', 'timestamp': today + relativedelta(days=-3, seconds=3)},
            {'user_id': '001', 'timestamp': today + relativedelta(days=-3, seconds=5, microseconds=999)},
            {'user_id': '001', 'timestamp': today + relativedelta(days=-3)},

            {'user_id': '002', 'timestamp': today + relativedelta(days=-3)},
            {'user_id': '002', 'timestamp': today + relativedelta(days=-3, seconds=7)},
            {'user_id': '002', 'timestamp': today + relativedelta(days=-3, seconds=6)},
        ]
        rdd = self.sc.parallelize(data)
        res = module.get_time_spent(rdd, page_time=10)

        partitions_data = res.glom().collect()
        self.assertTrue([('000', 16)] in partitions_data)
        self.assertTrue([('001', 15)] in partitions_data)
        self.assertTrue([('002', 17)] in partitions_data)


class DaysActiveTestCase(BaseSparkTestCase):

    def test_sort(self):
        today = module.get_day(datetime.utcnow())
        data = [
            {'user_id': '000', 'timestamp': today + relativedelta(days=-6)},
            {'user_id': '000', 'timestamp': today + relativedelta(days=-6, seconds=41)},
            {'user_id': '000', 'timestamp': today + relativedelta(days=-2)},
            {'user_id': '000', 'timestamp': today + relativedelta(days=-2, seconds=32)},
            {'user_id': '000', 'timestamp': today + relativedelta(days=-2, seconds=47)},
            {'user_id': '000', 'timestamp': today + relativedelta(days=-1)},
        ]
        rdd = self.sc.parallelize(data)
        res = module.get_days_active(rdd)

        rows = res.collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], '000')
        self.assertEqual(sorted(rows[0][1]), [
                today + relativedelta(days=-6),
                today + relativedelta(days=-2),
                today + relativedelta(days=-1),
        ])


class ViewedPagesCountsTestCase(BaseSparkTestCase):

    def test_sort(self):
        data = [
            {'user_id': '000', 'name': 'page1'},
            {'user_id': '000', 'name': 'page2'},
            {'user_id': '000', 'name': 'page3'},
            {'user_id': '000', 'name': 'page1'},
            {'user_id': '000', 'name': 'page1'},
            {'user_id': '000', 'name': 'page3'},
            {'user_id': '000', 'name': 'page3'},
            {'user_id': '000', 'name': 'page3'},
            {'user_id': '000', 'name': 'page2'},

            {'user_id': '001', 'name': 'page1'},
            {'user_id': '001', 'name': 'page3'},
            {'user_id': '001', 'name': 'page1'},
        ]
        rdd = self.sc.parallelize(data)
        res = module.get_viewed_pages_counts(rdd)

        rows = sorted(res.collect())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], '000')
        self.assertEqual(rows[0][1], {'page1': 3, 'page2': 2, 'page3': 4})
        self.assertEqual(rows[1][0], '001')
        self.assertEqual(rows[1][1], {'page1': 2, 'page3': 1})


class StatsTestCase(BaseSparkTestCase):

    def test_stats(self):
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
                'name': 'page1',
                'timestamp': today + relativedelta(days=-1),
            },
            {
                'user_id': '456',
                'name': 'page2',
                'timestamp': today + relativedelta(days=-4),
            },
            {
                'user_id': '456',
                'name': 'page1',
                'timestamp': today + relativedelta(days=-4, seconds=2),
            },
            {
                'user_id': '456',
                'name': 'page2',
                'timestamp': today + relativedelta(days=-2),
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
        self.assertEqual(sorted(rows[0]['days_active']), [
                today + relativedelta(days=-3),
                today + relativedelta(days=-1),
        ])
        self.assertEqual(rows[0]['viewed_pages_counts'], {
            'page1': 2,
            'page2': 1,
        })

        self.assertEqual(rows[1]['user_id'], '456')
        self.assertEqual(rows[1]['time_spent'], 22)
        self.assertEqual(sorted(rows[1]['days_active']), [
                today + relativedelta(days=-4),
                today + relativedelta(days=-2),
        ])
        self.assertEqual(rows[1]['viewed_pages_counts'], {
            'page1': 1,
            'page2': 2,
        })


if __name__ == '__main__':
    unittest.main()
