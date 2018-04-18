from datetime import datetime
from math import ceil
from copy import deepcopy
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

# TODO: use a single time spent algorithm in batch and real time
def get_sessions_time(timestamps, page_time, sort_ts=False):

    def iter_times():
        sorted_timestamps = sorted(timestamps) \
                if sort_ts else timestamps
        begin = sorted_timestamps[0]
        end = begin + page_time
        for ts in sorted_timestamps:
            if ts < end:
                end = ts + page_time
            else:
                yield end - begin
                begin = ts
                end = ts + page_time
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
    return get_sessions_time(timestamps,
            page_time=PAGE_TIME, sort_ts=True)

def _get_rt_days_active(pages_col, request,
        per_page=100):

    def iter_values():
        for docs in _iter_rt_paginated_docs(pages_col,
                request, per_page=per_page):
            yield list(set([get_day(d['timestamp']) for d in docs]))

    return reduce(lambda x, y: list(set(x + y)),
            list(iter_values()))

def _get_rt_viewed_pages_counts(pages_col, request,
        per_page=100):

    def iter_docs():
        for docs in _iter_rt_paginated_docs(pages_col,
                request, per_page=per_page):
            for doc in docs:
                yield doc

    counts = {}
    for doc in iter_docs():
        counts.setdefault(doc['name'], 0)
        counts[doc['name']] += 1
    return counts

def _merge_aggregated_data(user_id, rt_data):
    agg_pages_col = get_mongo_collection('aggregated_pages')
    agg_data = agg_pages_col.find_one({
            'user_id': user_id,
            }, sort=[('timestamp', -1)])
    if agg_data:
        agg_distinct_viewed_pages = agg_data['distinct_viewed_pages']
        agg_time_spent = agg_data['time_spent']
        agg_days_active = agg_data['days_active']
        agg_viewed_pages_counts = agg_data['viewed_pages_counts']
    else:
        agg_distinct_viewed_pages = []
        agg_time_spent = 0
        agg_days_active = []
        agg_viewed_pages_counts = {}

    # Merge real time and aggregated data
    distinct_viewed_pages = list(set(
            rt_data['distinct_viewed_pages'] + agg_distinct_viewed_pages))
    time_spent = rt_data['time_spent'] + agg_time_spent
    days_active = list(set(rt_data['days_active'] + agg_days_active))

    viewed_pages_counts = deepcopy(rt_data['viewed_pages_counts'])
    for name, count in agg_viewed_pages_counts.iteritems():
        viewed_pages_counts.setdefault(name, 0)
        viewed_pages_counts[name] += count
    most_viewed_page = sorted([(v, k)
            for k, v in viewed_pages_counts.items()])[-1][1]

    return {
        'user_id': user_id,
        'number_pages_viewed_in_the_last_7_days': len(distinct_viewed_pages),
        'time_spent_on_site_in_last_7_days': time_spent,
        'number_of_days_active_in_last_7_days': len(days_active),
        'most_viewed_page_in_last_7_days': most_viewed_page,
    }

def get_behavioral_profile(user_id):
    pages_col = get_mongo_collection('pages')
    rt_request = {
        'user_id': user_id,
        'timestamp': {'$gte': get_day(datetime.utcnow())},
    }
    rt_data = {
        'distinct_viewed_pages': _get_rt_distinct_viewed_pages(
                pages_col, rt_request),
        'time_spent': _get_rt_time_spent(
                pages_col, rt_request),
        'days_active': _get_rt_days_active(
                pages_col, rt_request),
        'viewed_pages_counts': _get_rt_viewed_pages_counts(
                pages_col, rt_request),
    }
    return _merge_aggregated_data(user_id, rt_data)
