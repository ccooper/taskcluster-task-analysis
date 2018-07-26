#!/usr/bin/env python
""" cost_per_run.py

    Methodology:

    We are merging two distinct data sources here: task information collected
    by the taskcluster-task-analysis module, and monthly cost data as reported
    by AWS.

    We sync these two data sources on the basis of worker type information
    rather than instance type data because worker types can run on multiple
    different instance types depending on the instance type costs at the time
    of provisioning. Worker type is tracked directly by Taskcluster, whereas
    it is tracked in AWS using the WorkerType tag.

    Cost data is currently imported monthly by hand from AWS via CSV using the
    "Monthly EC2 running hours costs and usage" report in Cost Explorer.

    TODO:
    * incorporate estimates for worker types that run on Mozilla-hosted
      hardware in our datacentre
    * automate import of cost data from AWS
    * clean up both Taskcluster and AWS data sources with better tagging
"""

import argparse
import simplejson as json
import os
import psycopg2
import sys
import time

from db_config import db_config
from psycopg2 import extras


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


def open_connection():
    try:
        db_params = db_config()
        psycopg2.extensions.set_wait_callback(extras.wait_select)
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        return conn, cur
    except:
        print "I am unable to connect to the database"
        sys.exit(1)


def close_connection(conn, cur):
    if cur:
        cur.close()
    if conn:
        conn.close()


def new_efficiency_worker_type():
    worker_type = {}
    worker_type['aws_hours'] = 0
    worker_type['tc_hours'] = 0
    worker_type['factor'] = 1
    return worker_type


@timeit
def get_efficiency_factor(year, month):
    """The efficiency factor is a measure of the discrepancy between the total time reported for
    each worker type and the total time billed by AWS for that same worker type. This accounts
    for the overhead involved in setting up & tearing down workers, and also time spent just
    waiting for new tasks.

    Each worker type will have its own overhead, so we calculate this per worker type.
    """

    efficiency = {}
    # Get hours per worker_type reported by AWS
    query = "SELECT worker_type, usage_hours \
            FROM worker_type_monthly_costs \
            WHERE year = %d \
            AND month = %d \
            ORDER BY usage_hours DESC" % (year, month)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        worker_type = row[0]
        hours = row[1]
        efficiency[worker_type] = new_efficiency_worker_type()
        efficiency[worker_type]['aws_hours'] = hours

    # Get hours per worker_type reported by Taskcluster
    query = "SELECT worker_type, SUM(duration)/1000/60/60 AS total_hours \
             FROM tasks \
             WHERE DATE_PART('year', created) = %d \
             AND DATE_PART('month', created) = %d \
             GROUP BY worker_type \
             ORDER BY total_hours DESC" % (year, month)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        worker_type = row[0]
        hours = row[1]
        if hours == 0:
            continue
        if worker_type not in efficiency:
            efficiency[worker_type] = new_efficiency_worker_type()
        efficiency[worker_type]['tc_hours'] = hours
        if efficiency[worker_type]['tc_hours'] != 0 and efficiency[worker_type]['aws_hours'] != 0:
            efficiency[worker_type]['factor'] = \
              efficiency[worker_type]['aws_hours'] / efficiency[worker_type]['tc_hours']

    return efficiency


@timeit
def get_num_pushes(branch, year, month):
    query = "SELECT COUNT(DISTINCT(revision)) \
             FROM tasks \
             WHERE project = '%s' \
             AND DATE_PART('year', created) = %d \
             AND DATE_PART('month', created) = %d" % (branch, year, month)
    cur.execute(query)
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        return 0


@timeit
def get_monthly_worker_type_costs(year, month):
    query = "SELECT provisioner, worker_type, usage_hours, cost \
             FROM worker_type_monthly_costs \
             WHERE year = %d AND month = %d" % (year, month)
    cur.execute(query)
    rows = cur.fetchall()
    worker_type_costs = {}
    for row in rows:
        worker_type = row[1]
        if worker_type in worker_type_costs:
            worker_type_costs[worker_type]['provisioner'].append(row[0])
            worker_type_costs[worker_type]['total_hours'] += row[2]
            worker_type_costs[worker_type]['cost'] += row[3]
        else:
            worker_type_costs[worker_type] = {}
            worker_type_costs[worker_type]['provisioner'] = [row[0]]
            worker_type_costs[worker_type]['total_hours'] = row[2]
            worker_type_costs[worker_type]['cost'] = row[3]
            worker_type_costs[worker_type]['branch_hours'] = 0
    return worker_type_costs


@timeit
def get_duration_per_worker_type(branch, year, month):
    query = "SELECT worker_type, SUM(duration/(1000*60*60)) \
             FROM tasks \
             WHERE project = '%s' AND \
             DATE_PART('year', created) = %d \
             AND DATE_PART('month', created) = %d \
             AND state = 'completed' \
             GROUP BY project, worker_type" % (branch, year, month)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        worker_type = row[0]
        branch_hours = row[1]
        if worker_type in worker_type_costs:
            worker_type_costs[worker_type]['branch_hours'] += branch_hours
        else:
            worker_type_costs[worker_type] = {}
            worker_type_costs[worker_type]['provisioner'] = []
            worker_type_costs[worker_type]['total_hours'] = 0
            worker_type_costs[worker_type]['cost'] = 0
            worker_type_costs[worker_type]['branch_hours'] = branch_hours


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--branch",
                        help="Branch to query, e.g. mozilla-central, try, ...",
                        required=True,
                        type=str)
    parser.add_argument("--month",
                        help="Month to process, format: YYYY-MM",
                        required=True,
                        type=str)
    args = parser.parse_args()

    branch = args.branch
    year, month = args.month.split('-', 2)
    year = int(year)
    month = int(month)
    if not year or not month or month < 1 or month > 12:
        print "ERROR: unable to parse month"
        sys.exit(1)

    # The main db query can be expensiv, so try to use
    # cached data if we've run with these params before.
    cost_json_file = "logs/cost-%s-%d-%02d.json" % (branch, year, month)
    data = {}
    try:
        with open(cost_json_file) as f:
            data = json.load(f)
    except:
        pass

    efficiency_json_file = "logs/efficiency-%d-%02d.json" % (year, month)
    efficiency = {}
    try:
        with open(efficiency_json_file) as f:
            efficiency = json.load(f)
    except:
        pass

    conn = None
    cur = None
    if data and 'num_pushes' in data and 'worker_type_costs' in data:
        num_pushes = data['num_pushes']
        worker_type_costs = data['worker_type_costs']
    else:
        # Fetch our cost data from the db instead,
        conn, cur = open_connection()
        num_pushes = get_num_pushes(branch, year, month)
        worker_type_costs = get_monthly_worker_type_costs(year, month)
        get_duration_per_worker_type(branch, year, month)

    if not efficiency:
        if not conn:
            conn, cur = open_connection()
        efficiency = get_efficiency_factor(year, month)

    close_connection(conn, cur)

    total_cost = 0
    for worker_type in worker_type_costs:
        if worker_type_costs[worker_type]['total_hours'] != 0:
            efficiency_factor = 1
            if worker_type in efficiency:
                efficiency_factor = efficiency[worker_type]['factor']
            total_cost += float(worker_type_costs[worker_type]['cost']) / \
                float(worker_type_costs[worker_type]['total_hours']) * \
                float(worker_type_costs[worker_type]['branch_hours']) * \
                float(efficiency_factor)
    cost_per_push = total_cost / num_pushes

    print "Total spend for %s: %s" % (branch, '${:,.2f}'.format(total_cost))
    print "Total # of pushes:  %d" % num_pushes
    print "Cost per push:      %s" % '${:,.2f}'.format(cost_per_push)

    # Cache our data to a json file
    if not os.path.exists(cost_json_file):
        data = {}
        data['num_pushes'] = num_pushes
        data['efficiency'] = efficiency
        data['worker_type_costs'] = worker_type_costs
        with open(cost_json_file, 'w') as f:
            json.dump(data, f)

    if not os.path.exists(efficiency_json_file):
        with open(efficiency_json_file, 'w') as f:
            json.dump(efficiency, f)
