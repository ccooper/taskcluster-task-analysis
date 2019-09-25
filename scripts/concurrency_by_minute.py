#!/usr/bin/env python

import argparse
import csv
import json
import os
import psycopg2
import sys

from db_config import db_config
from datetime import date, datetime, timedelta
from psycopg2 import extras
from shared import timeit

aws_windows_workers = [
'g2.2xlarge',
'g3.4xlarge',
'm5d.2xlarge',
'c5.xlarge',
'c5d.18xlarge',
'c4.2xlarge',
'm3.2xlarge',
'g3s.xlarge',
'c5.4xlarge',
'c5.2xlarge',
]

psycopg2.extensions.set_wait_callback(extras.wait_select)


def initialize_entry():
    entry = {}
    for instance in aws_windows_workers:
        entry[instance] = 0
    return entry


@timeit
def get_concurrent_tasks_for_timerange(start_ts, end_ts):
    """ Returns the number of concurrent tasks for the period between two timestamps
        WARNING: Calculating concurrent tasks is computationally expensive. If the
        range between start_ts and end_ts gets larger, it may take considerably more
        time to generate a result.
    """
    entry = initialize_entry()
    query = (
       "SELECT w.instance_type, COUNT(t.task_id) AS num_instances \
        FROM tasks_windows_201908 t, worker_instance_mapping w \
        WHERE t.started<=timestamp'%s' \
        AND t.resolved>=timestamp'%s' \
        AND t.worker_type=w.worker_type \
        GROUP BY w.instance_type"
        % (end_ts, start_ts)
    )
    cur.execute(query)
    records = cur.fetchall()
    for record in records:
        entry[record[0]] += record[1]
    return entry


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        help='Start timestamp, format="YYYY-MM-DD HH:mm"',
        type=str,
        required=True,
    )
    parser.add_argument(
        "--end",
        help='Start timestamp, format="YYYY-MM-DD HH:mm"',
        type=str,
        required=True,
    )
    args = parser.parse_args()
    if not args.start:
        print('Must supply a start timestamp, format="YYYY-MM-DD HH:mm"')
        sys.exit(1)
    if not args.end:
        print('Must supply an end timestamp, format="YYYY-MM-DD HH:mm"')
        sys.exit(2)
    user_start = datetime.strptime(args.start, '%Y-%m-%d %H:%M')
    user_end = datetime.strptime(args.end, '%Y-%m-%d %H:%M')

    conn = None
    db_params = db_config()
    try:
        conn = psycopg2.connect(**db_params)
    except psycopg2.Error:
        print("I am unable to connect to the database")
        sys.exit(3)
    cur = conn.cursor()
    current_start = user_start

    csvfiles = {}
    for instance in aws_windows_workers:
        filename = "data/concurrent_%s_by_minute_201908.csv" % instance
        csvfile = open(filename, 'a')
        my_writer = csv.writer(csvfile)
        csvfiles[instance] = {}
        csvfiles[instance]['csvfile'] = csvfile
        csvfiles[instance]['writer'] = my_writer
    while current_start < user_end:
        print(current_start)
        current_end = current_start + timedelta(seconds=59)
        entry = get_concurrent_tasks_for_timerange(current_start, current_end)
        for instance in entry:
            csvfiles[instance]['writer'].writerow([current_start, current_end, entry[instance]])
            csvfiles[instance]['csvfile'].flush()
        current_start = current_start + timedelta(minutes=1)
    for instance in aws_windows_workers:
        csvfiles[instance]['csvfile'].close()
