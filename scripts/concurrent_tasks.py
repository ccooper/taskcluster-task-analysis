#!/usr/bin/env python

import argparse
import calendar

import json
import os
# import pprint
import psycopg2
import sys

from db_config import db_config
from datetime import date, datetime, timedelta
from psycopg2 import extras
from shared import log_ts, timeit

psycopg2.extensions.set_wait_callback(extras.wait_select)


@timeit
def get_concurrent_tasks_for_day(my_date):
    """ Returns a list of tuples of the tasks with the highest concurrency for a given day
    """
    query = (
        "SELECT * \
            FROM task_overlaps_day('%s')"
        % my_date
    )
    cur.execute(query)
    records = cur.fetchall()
    return records


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days + 1)):
        yield start_date + timedelta(n)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-r", "--refresh-json", help="Refresh JSON on disk", action="store_true"
    )
    parser.add_argument(
        "--year_month",
        help='Month to process, format="YYYY-MM"',
        type=str,
        required=True,
    )
    args = parser.parse_args()
    if not args.year_month:
        print('Must supply a month to process, format="YYYY-MM"')
        sys.exit(1)

    localfile = "logs/concurrent_tasks_%s.json" % args.year_month
    concurrent_tasks_by_day = {}
    if os.path.exists(localfile):
        with open(localfile) as ct:
            concurrent_tasks_by_day = json.load(ct)

    year, month = map(int, args.year_month.split("-"))
    first_day = date(year, month, 1)
    _, num_days = calendar.monthrange(first_day.year, first_day.month)
    last_day = date(year, month, num_days)

    conn = None
    db_params = db_config()
    today = datetime.now().date()
    for working_date in daterange(first_day, last_day):
        single_date = str(working_date)
        if working_date > today:
            print("[%s] Skipping %s because it hasn't happened yet" % (log_ts(), single_date))
            continue
        if working_date == today:
            print("[%s] Skipping %s because today isn't over yet." % (log_ts(), single_date))
            continue
        print("[%s] Processing %s..." % (log_ts(), single_date))
        if single_date not in concurrent_tasks_by_day:
            try:
                conn = psycopg2.connect(**db_params)
            except psycopg2.Error:
                print("I am unable to connect to the database")
                sys.exit(2)
            cur = conn.cursor()
            concurrent_tasks_by_day[single_date] = get_concurrent_tasks_for_day(single_date)
            cur.close()
            conn.close()
            with open(localfile, "w") as ct:
                json.dump(concurrent_tasks_by_day, ct, indent=4, sort_keys=True, default=str)
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(concurrent_tasks_by_day)
    # m = max(max(concurrent_tasks_by_day[day][1])
    m = max(j for day in concurrent_tasks_by_day for i, j in concurrent_tasks_by_day[day])
    print("Maximum concurrent tasks: %s" % "{:,}".format(int(m)))
