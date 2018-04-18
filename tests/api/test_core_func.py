#!/usr/bin/env python
import unittest
from datetime import datetime

from dateutil.relativedelta import relativedelta

from pymongo.database import CollectionInvalid

from mock import Mock, patch

from api import core, utils


class BaseMongoTestCase(unittest.TestCase):

    def setUp(self):
        client = utils._get_mongo_client()
        self.db = client['tests']
        self.setup_col('pages')
        self.setup_col('aggregated_pages')

        self.call_number = 0

    # def tearDown(self):
    #     self.db.drop_collection('pages')
    #     self.db.drop_collection('aggregated_pages')

    def setup_col(self, col_name):
        try:
            self.db.create_collection(col_name)
        except CollectionInvalid:
            pass
        self.db.drop_collection(col_name)

    def side_get_mongo_collection(self, *args, **kwargs):
        self.call_number += 1

        if self.call_number == 1:
            return self.db['pages']
        else:
            return self.db['aggregated_pages']


class BehavioralProfileTestCase(BaseMongoTestCase):

    def test_rt_distinct_page_names(self):
        today = core.get_day(datetime.utcnow())
        docs = [
            {
                'user_id': '123',
                'name': 'page1',
                'timestamp': today + relativedelta(seconds=1),

            },
            {
                'user_id': '123',
                'name': 'page2',
                'timestamp': today + relativedelta(seconds=4),
            },
            {
                'user_id': '123',
                'name': 'page1',
                'timestamp': today + relativedelta(seconds=18),
            },
            {
                'user_id': '123',
                'name': 'page1',
                'timestamp': today + relativedelta(seconds=23),
            },
        ]
        self.db['pages'].insert_many(docs)

        with patch.object(core, 'get_mongo_collection') as mock_get_mongo_collection:
            mock_get_mongo_collection.side_effect = self.side_get_mongo_collection

            res = core.get_behavioral_profile('123')

        self.assertEqual(res['user_id'], '123')
        self.assertEqual(res['number_pages_viewed_in_the_last_7_days'], 2)
        self.assertEqual(res['time_spent_on_site_in_last_7_days'], 28)
        self.assertEqual(res['number_of_days_active_in_last_7_days'], 1)

    def test_rt_limit(self):

        def iter_pages_docs():
            for i in xrange(1001):
                yield {
                    'user_id': '123',
                    'name': 'page%s' % i,
                    'timestamp': datetime(2018, 4, 12) + relativedelta(seconds=i)
                }

        self.db['pages'].insert_many(list(iter_pages_docs()))

        with patch.object(core, 'get_day') as mock_get_day, \
                patch.object(core, 'get_mongo_collection') as mock_get_mongo_collection:
            mock_get_day.return_value = datetime(2018, 4, 12)
            mock_get_mongo_collection.side_effect = self.side_get_mongo_collection

            res = core.get_behavioral_profile('123')

        self.assertEqual(res['user_id'], '123')
        self.assertEqual(res['number_pages_viewed_in_the_last_7_days'], 1001)


if __name__ == '__main__':
    unittest.main()
