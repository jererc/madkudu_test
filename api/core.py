from datetime import datetime
from math import ceil
import logging

from utils import get_mongo_collection, parse_ts, get_day, get_ts


DOCS_COUNT_HARD_LIMIT = 1000000
PAGE_TIME = 10  # session duration in seconds

logger = logging.getLogger(__name__)


def insert_page_view_event(user_id, name, timestamp):
    pages_col = get_mongo_collection('pages')
    doc = {
        'user_id': user_id,
        'name': name,
        'timestamp': parse_ts(timestamp),
    }
    return pages_col.insert(doc)

def _iter_rt_paginated_docs(pages_col, request,
        per_page=100):

    def get_page_docs(page):
        cur = pages_col.find(request,
                skip=(page - 1) * per_page,
                limit=per_page,
                sort=[('timestamp', 1)])
        return [d for d in cur]

    total_docs = min(pages_col.find(request).count(),
            DOCS_COUNT_HARD_LIMIT)
    pages = int(ceil(total_docs / float(per_page)))
    for page in range(1, pages + 2):
        yield get_page_docs(page)

def _get_rt_distinct_viewed_pages(pages_col, request,
        per_page=100):

    def iter_values():
        for docs in _iter_rt_paginated_docs(pages_col,
                request, per_page=per_page):
            yield list(set([d['name'] for d in docs]))

    return reduce(lambda x, y: list(set(x + y)),
            list(iter_values()))

# TODO: use single time spent algorithms in batch amd real time
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

def _get_rt_time_spent(pages_col, request,
        per_page=100):

    def iter_values():
        for docs in _iter_rt_paginated_docs(pages_col,
                request, per_page=per_page):
            yield [get_ts(d['timestamp']) for d in docs]

    timestamps = reduce(lambda x, y: x + y,
            list(iter_values()))
    return get_sessions_time(timestamps, page_time=PAGE_TIME)

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
    rt_time_spent = _get_rt_time_spent(pages_col, rt_request)

    # Aggregated data
    agg_data = agg_pages_col.find_one({
            'user_id': user_id,
            }, sort=[('timestamp', -1)])
    if agg_data:
        agg_distinct_viewed_pages = agg_data['distinct_viewed_pages']
        agg_time_spent = agg_data['time_spent']
    else:
        agg_distinct_viewed_pages = []
        agg_time_spent = 0

    # Merge
    distinct_viewed_pages = list(set(
            rt_distinct_viewed_pages + agg_distinct_viewed_pages))
    time_spent = rt_time_spent + agg_time_spent

    res = {
        'user_id': user_id,
        'number_pages_viewed_in_the_last_7_days': len(distinct_viewed_pages),
        'time_spent_on_site_in_last_7_days': time_spent,
        # 'number_of_days_active_in_last_7_days': 3,
        # 'most_viewed_page_in_last_7_days': 'Blog: better B2B customer experience',
    }
    return res
