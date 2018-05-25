#!/usr/bin/env python

import argparse
import json
import os
import psycopg2
import requests
import sys
import time

from db_config import db_config
from datetime import datetime, timedelta
from psycopg2 import extras
from scipy import stats

REPO = "mozilla-central"
PUSHES_DIR = "logs"

HASHTAGS = [
        "#Mozilla",
        "#ContinuousIntegration",
        "#Taskcluster"
        ]

psycopg2.extensions.set_wait_callback(extras.wait_select)


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print '%r  %2.2f s' % \
                  (method.__name__, (te - ts))
        return result

    return timed


def download_push_data(url, localfile):
    resp = requests.get(url=url)
    data = json.loads(resp.text)
    if data:
        with open(localfile, 'w') as outfile:
            json.dump(data, outfile)


def get_last_day_of_previous_month(from_date=None):
    if not from_date:
        from_date = datetime.now()
    first = from_date.replace(day=1)
    return first - timedelta(days=1)


def get_first_day_of_month(from_date=None):
    if not from_date:
        from_date = datetime.now()
    return from_date.replace(day=1)


def get_merge_csets(daterange):
    if not os.path.exists(PUSHES_DIR):
        os.makedirs(PUSHES_DIR)
    pushes_json = "%s-pushes-%s.json" % (REPO, daterange[:7])
    pushes_path = os.path.join(PUSHES_DIR, pushes_json)
    if not os.path.isfile(pushes_path) or args.refresh_json:
        # Download the push data as a json blob
        first_day, last_day = daterange.split(' to ')
        url = "https://hg.mozilla.org/%s/json-pushes?full=1&startdate=%s&enddate=%s" % \
            (REPO, first_day, last_day)
        print "Donwloading %s data for %s" % (REPO, daterange)
        download_push_data(url, pushes_path)

    with open(pushes_path) as pp:
        pushes = json.load(pp)
    merges = []
    if pushes:
        for push in pushes:
            p = pushes[push]
            for cset in p['changesets']:
                if cset['desc'].startswith('Merge inbound'):
                    merges.append(cset['node'])
    return merges


def convert_daterange_to_string(daterange):
    return daterange.replace(" ", "_")


@timeit
def avg_duration(merges):
    formatted_merges = "'" + "', '".join(merges) + "'"
    query = "SELECT revision, SUM(duration)/1000/60/60 \
            FROM tasks \
            WHERE revision IN (%s) \
            GROUP BY revision" % formatted_merges
    cur.execute(query)
    records = cur.fetchall()
    durations = [record[1] for record in records]
    return float(round(stats.hmean(durations), 1))


@timeit
def end_to_end(merges):
    formatted_merges = "'" + "', '".join(merges) + "'"
    query = "SELECT EXTRACT(EPOCH FROM (MAX(resolved)-MIN(started))) \
            FROM tasks \
            WHERE revision IN (%s) \
            GROUP BY revision" % formatted_merges
    cur.execute(query)
    records = cur.fetchall()
    e2e_secs = [record[0] for record in records]
    # We want to convert our value in seconds to hours for display.
    return float(round(stats.hmean(e2e_secs)/60/60, 1))


@timeit
def tasks_per_month(year, month):
    query = "SELECT COUNT(task_id) \
            FROM tasks \
            WHERE DATE_PART('year', created) = %s \
            AND DATE_PART('month', created) = %s" % (year, month)
    cur.execute(query)
    records = cur.fetchone()
    if records:
        # print "# of tasks: %d" % records[0]
        return records[0]
    else:
        return 0


@timeit
def compute_years_per_month(year, month):
    query = "SELECT SUM(duration)/1000/60/60/24/365 \
            FROM tasks \
            WHERE DATE_PART('year', created) = %s \
            AND DATE_PART('month', created) = %s" % (year, month)
    cur.execute(query)
    records = cur.fetchone()
    if records:
        # print "Compute years: %s" % records[0]
        return records[0]
    else:
        return 0


@timeit
def unique_workers_per_month(year, month):
    query = "SELECT COUNT(DISTINCT worker_id) \
            FROM tasks \
            WHERE DATE_PART('year', created) = %s \
            AND DATE_PART('month', created) = %s" % (year, month)
    cur.execute(query)
    records = cur.fetchone()
    if records:
        # print "# of unique workers: %d" % records[0]
        return records[0]
    else:
        return 0


def format_numtasks_tweet(first_day, num_tasks, compute_years, num_workers):
    tweet = "Firefox CI in %s: %s tasks; %.1f compute years; %s unique workers" % \
        (first_day.strftime("%B %Y"), "{:,}".format(int(num_tasks)), compute_years, "{:,}".format(int(num_workers)))
    for hashtag in HASHTAGS:
        tweet += " " + hashtag
    return tweet


def format_endtoend_tweet(first_day, end_to_end_time):
    tweet = "Average end-to-end time per merge commit for %s: %.1f hours" % \
        (first_day.strftime("%B %Y"), end_to_end_time)
    for hashtag in HASHTAGS:
        tweet += " " + hashtag
    return tweet


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--refresh-json", help="Refresh JSON on disk", action="store_true")
    parser.add_argument("--daterange",
                        help="Daterange to process, format=\"YYYY-MM-DD to YYYY-MM-DD\"",
                        type=str)
    args = parser.parse_args()

    conn = None
    try:
        db_params = db_config()
        conn = psycopg2.connect(**db_params)
    except:
        print "I am unable to connect to the database"
        sys.exit(1)

    cur = conn.cursor()

    if args.daterange:
        daterange = args.daterange
    else:
        # If a daterange is not provided, assume the user want to process date for the
        # previous month and construct the appropriate daterange
        last_day = get_last_day_of_previous_month()
        first_day = get_first_day_of_month(last_day)
        daterange = first_day.strftime("%Y-%m-%d") + " to " + last_day.strftime("%Y-%m-%d")

    print "Processing %s" % daterange
    merges = get_merge_csets(daterange)
    # duration = avg_duration(merges)
    end_to_end_time = end_to_end(merges)

    first_date, last_date = daterange.split(' to ')
    first_day = datetime.strptime(first_date, "%Y-%m-%d")
    year = first_day.strftime("%Y")
    month = first_day.strftime("%m")
    num_tasks = tasks_per_month(year, month)
    compute_years = compute_years_per_month(year, month)
    num_workers = unique_workers_per_month(year, month)

    cur.close()
    conn.close()

    print format_numtasks_tweet(first_day, num_tasks, compute_years, num_workers)
    print format_endtoend_tweet(first_day, end_to_end_time)
