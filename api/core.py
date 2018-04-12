from datetime import datetime
from math import ceil
import logging

from utils import get_mongo_collection, parse_ts, get_day


DOCS_COUNT_HARD_LIMIT = 1000000

logger = logging.getLogger(__name__)


def insert_page_view_event(user_id, name, timestamp):
    pages_col = get_mongo_collection('pages')
    doc = {
        'user_id': user_id,
        'name': name,
        'timestamp': parse_ts(timestamp),
    }
    return pages_col.insert(doc)

def _get_rt_distinct_viewed_pages(pages_col, request,
        per_page=100):

    def get_paginated_data(page):
        cur = pages_col.find(request,
                skip=(page - 1) * per_page,
                limit=per_page,
                sort=[('timestamp', 1)])
        return list(set([d['name'] for d in cur]))

    def iter_paginated_page_names():
        total_docs = min(pages_col.find(request).count(),
                DOCS_COUNT_HARD_LIMIT)
        pages = int(ceil(total_docs / float(per_page)))
        for page in range(1, pages + 2):
            yield get_paginated_data(page)

    return reduce(lambda x, y: list(set(x + y)),
            list(iter_paginated_page_names()))

# TODO: implement missing stats
def get_behavioral_profile(user_id):
    today = get_day(datetime.utcnow())

    pages_col = get_mongo_collection('pages')
    agg_pages_col = get_mongo_collection('aggregated_pages')

    # Real time data
    rt_request = {
        'user_id': user_id,
        'timestamp': {'$gte': today},
    }
    rt_distinct_viewed_pages = _get_rt_distinct_viewed_pages(
            pages_col, rt_request)

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
