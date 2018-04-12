from datetime import datetime
import logging

from utils import get_mongo_collection, parse_ts, get_day


logger = logging.getLogger(__name__)


def insert_page_view_event(user_id, name, timestamp):
    pages_col = get_mongo_collection('pages')
    doc = {
        'user_id': user_id,
        'name': name,
        'timestamp': parse_ts(timestamp),
    }
    return pages_col.insert(doc)

# TODO: implement missing stats
def get_behavioral_profile(user_id):
    today = get_day(datetime.utcnow())

    pages_col = get_mongo_collection('pages')
    agg_pages_col = get_mongo_collection('aggregated_pages')

    # Real time data
    rt_cur = pages_col.find({
            'user_id': user_id,
            'timestamp': {'$gte': today},
            })
    rt_distinct_viewed_pages = list(set([d['name'] for d in rt_cur]))

    # Aggregated data
    agg_data = agg_pages_col.find_one({
            'user_id': user_id,
            }, sort=[('timestamp', -1)])
    if agg_data:
        agg_distinct_viewed_pages = agg_data['distinct_viewed_pages']
    else:
        agg_distinct_viewed_pages = []

    distinct_viewed_pages = list(set(
            rt_distinct_viewed_pages + agg_distinct_viewed_pages))

    res = {
        'user_id': user_id,
        'number_pages_viewed_in_the_last_7_days': len(distinct_viewed_pages),
        # 'time_spent_on_site_in_last_7_days': 18,
        # 'number_of_days_active_in_last_7_days': 3,
        # 'most_viewed_page_in_last_7_days': 'Blog: better B2B customer experience',
    }
    return res
