#!/usr/bin/env python

import argparse
import boto3
import copy
import csv
import json
import os
import pprint
import psycopg2
import re
import sys

from datetime import datetime
from db_config import db_config
from psycopg2 import extras
from shared import timeit

instance_type_query = {
    "TimePeriod": {"Start": "", "End": ""},
    "Granularity": "MONTHLY",
    "Filter": {
        "Dimensions": {"Key": "USAGE_TYPE_GROUP", "Values": ["EC2: Running Hours"]}
    },
    "Metrics": ["UnblendedCost", "UsageQuantity"],
    "GroupBy": [{"Type": "DIMENSION", "Key": "INSTANCE_TYPE"}],
}

worker_type_query = {
    "TimePeriod": {"Start": "", "End": ""},
    "Granularity": "MONTHLY",
    "Filter": {"Dimensions": {"Key": "INSTANCE_TYPE", "Values": []}},
    "Metrics": ["UnblendedCost", "UsageQuantity"],
    "GroupBy": [{"Type": "TAG", "Key": "WorkerType"}],
}


buckets = [
    ("Linux64", ["linux64"]),
    ("Linux32", ["linux32"]),
    ("OS X", ["osx"]),
    ("Android", ["android", "Android", "mobile"]),
    ("Windows Server 2012", ["windows2012", "win2012"]),
    ("Windows 7", ["windows7", "win7"]),
    ("Windows 10", ["windows10", "win10"]),
    ("b2g", ["mulet", "gaia", "b2g", "flame"]),
]

DATA_DIR = "./data"

pp = pprint.PrettyPrinter(indent=4)
psycopg2.extensions.set_wait_callback(extras.wait_select)
worker_type_duration_totals_tc = {}


def get_first_day_of_month(from_date=None):
    if not from_date:
        from_date = datetime.now()
    return from_date.replace(day=1)


def is_valid_date(in_date):
    try:
        datetime.strptime(in_date, "%Y-%m-%d")
        return True
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")
    return False


def split_worker_key(key):
    key = re.sub(r"^WorkerType\$", "", key)
    if key == "":
        return "None", "None"
    if key.find("/") == -1:
        return "None", key
    return key.split("/", 2)


def get_bucket_for_db_platform(worker_type, db_platform):
    for bucket, matches in buckets:
        if any(match in db_platform for match in matches):
            return bucket
    for bucket, matches in buckets:
        if any(match in worker_type for match in matches):
            return bucket
    return "Other"


@timeit
def get_instance_types(json_file, startdate, enddate):
    instance_types = {}
    if os.path.exists(json_file):
        with open(json_file) as infile:
            instance_types = json.load(infile)
    else:
        instance_type_query["TimePeriod"]["Start"] = args.startdate
        instance_type_query["TimePeriod"]["End"] = args.enddate
        client = boto3.client("ce")
        response = client.get_cost_and_usage(**instance_type_query)

        if response and "ResultsByTime" in response:
            for entry in response["ResultsByTime"][0]["Groups"]:
                key = entry["Keys"][0]
                instance_types[key] = {}
                instance_types[key]["cost"] = entry["Metrics"]["UnblendedCost"][
                    "Amount"
                ]
                instance_types[key]["hours"] = entry["Metrics"]["UsageQuantity"][
                    "Amount"
                ]
                instance_types[key]["worker_types"] = {}

    return instance_types


@timeit
def get_worker_types(json_file, instance_types, startdate, enddate):
    worker_types = {}
    if os.path.exists(json_file):
        with open(json_file) as infile:
            worker_types = json.load(infile)
    else:
        worker_type_query["TimePeriod"]["Start"] = args.startdate
        worker_type_query["TimePeriod"]["End"] = args.enddate
        client = boto3.client("ce")
        for instance_type in instance_types:
            current_worker_type_query = copy.deepcopy(worker_type_query)
            current_worker_type_query["Filter"]["Dimensions"]["Values"].append(
                instance_type
            )

            response = client.get_cost_and_usage(**current_worker_type_query)

            if response and "ResultsByTime" in response:
                for entry in response["ResultsByTime"][0]["Groups"]:
                    key = entry["Keys"][0]
                    provisioner, worker_type = split_worker_key(key)
                    if worker_type not in worker_types:
                        worker_types[worker_type] = {"cost": 0, "hours": 0}
                    worker_types[worker_type]["cost"] += float(
                        entry["Metrics"]["UnblendedCost"]["Amount"]
                    )
                    worker_types[worker_type]["hours"] += float(
                        entry["Metrics"]["UsageQuantity"]["Amount"]
                    )
                    instance_types[instance_type]["worker_types"][worker_type] = {
                        "provisioner": provisioner,
                        "cost": entry["Metrics"]["UnblendedCost"]["Amount"],
                        "hours": entry["Metrics"]["UsageQuantity"]["Amount"],
                    }

    return worker_types, instance_types


@timeit
def get_worker_type_durations(json_file, year, month):
    worker_type_durations = {}
    if os.path.exists(json_file):
        with open(json_file) as infile:
            worker_type_durations = json.load(infile)
    else:
        conn = None
        try:
            db_params = db_config()
            conn = psycopg2.connect(**db_params)
        except psycopg2.Error:
            print("I am unable to connect to the database")
            sys.exit(1)

        cur = conn.cursor()
        query = (
            "SELECT worker_type, platform, SUM(duration) AS total_time \
                FROM tasks \
                WHERE DATE_PART('year', created) = %d AND DATE_PART('month', created) = %d \
                AND provisioner = 'aws-provisioner-v1' \
                GROUP BY worker_type, platform \
                ORDER BY worker_type ASC, platform ASC, total_time DESC"
            % (year, month)
        )
        cur.execute(query)
        records = cur.fetchall()
        for record in records:
            if record:
                worker_type = record[0]
                platform = record[1]
                if not platform:
                    platform = "None"
                duration_ms = record[2]
                if worker_type not in worker_type_durations:
                    worker_type_durations[worker_type] = {}
                    worker_type_durations[worker_type]["total"] = 0
                worker_type_durations[worker_type][platform] = duration_ms
                worker_type_durations[worker_type]["total"] += duration_ms

        cur.close()
        conn.close()

    return worker_type_durations


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--startdate",
        required=True,
        type=str,
        help="Start date for query, format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--enddate",
        required=True,
        type=str,
        help="End date for query, format: YYYY-MM-DD",
    )
    args = parser.parse_args()
    if not is_valid_date(args.startdate):
        parser.print_help(sys.stderr)
        sys.exit(1)
    if not is_valid_date(args.enddate):
        parser.print_help(sys.stderr)
        sys.exit(2)
    if args.startdate > args.enddate:
        sys.stderr.write("Start date is later than end date")
        sys.exit(3)

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    instance_types_file = os.path.join(
        DATA_DIR, "instance_types_" + args.startdate + "_" + args.enddate + ".json"
    )
    worker_types_file = os.path.join(
        DATA_DIR, "worker_types_" + args.startdate + "_" + args.enddate + ".json"
    )
    worker_type_durations_file = os.path.join(
        DATA_DIR,
        "worker_type_durations_" + args.startdate + "_" + args.enddate + ".json",
    )

    first_day = datetime.strptime(args.startdate, "%Y-%m-%d")
    year = int(first_day.strftime("%Y"))
    month = int(first_day.strftime("%m"))

    instance_types = get_instance_types(
        instance_types_file, args.startdate, args.enddate
    )
    worker_types, instance_types = get_worker_types(
        worker_types_file, instance_types, args.startdate, args.enddate
    )
    worker_type_durations = get_worker_type_durations(
        worker_type_durations_file, year, month
    )

    # calculate overhead
    for worker_type in worker_types:
        if worker_type in worker_type_durations:
            tc_hours = (
                float(worker_type_durations[worker_type]["total"])
                / 1000.0
                / 60.0
                / 60.0
            )
            aws_hours = float(worker_types[worker_type]["hours"])
            if not aws_hours:
                continue
            overhead = (1.0 - tc_hours / aws_hours) * 100.0
            # print("worker type: %s - TC hours: %f - AWS hours - %f - overhead: %.2f - cost: $%.2f" %
            #       (worker_type, tc_hours, aws_hours, overhead, worker_types[worker_type]['cost']))

    # sort worker types by cost
    # for worker_type in sorted(worker_types, key=lambda x: (worker_types[x]['cost']), reverse=True):
    #    print("worker type: %s - cost: $%.2f" % (worker_type, worker_types[worker_type]['cost']))

    # Calculate cost per platforms by bucket
    platform_buckets = {}
    for worker_type in worker_type_durations:
        for platform in worker_type_durations[worker_type]:
            bucket = get_bucket_for_db_platform(worker_type, platform)
            if bucket not in platform_buckets:
                platform_buckets[bucket] = {}
                platform_buckets[bucket]["bucket_msecs"] = 0
                platform_buckets[bucket]["bucket_cost"] = 0
                platform_buckets[bucket]["worker_types"] = {}
            platform_buckets[bucket]["bucket_msecs"] += worker_type_durations[
                worker_type
            ][platform]
            if worker_type not in platform_buckets[bucket]["worker_types"]:
                platform_buckets[bucket]["worker_types"][worker_type] = {
                    "platforms": {},
                    "cost": 0,
                    "msecs": 0,
                }
            platform_buckets[bucket]["worker_types"][worker_type][
                "msecs"
            ] += worker_type_durations[worker_type][platform]
            if (
                platform
                not in platform_buckets[bucket]["worker_types"][worker_type][
                    "platforms"
                ]
            ):
                platform_buckets[bucket]["worker_types"][worker_type]["platforms"][
                    platform
                ] = {}
                platform_buckets[bucket]["worker_types"][worker_type]["platforms"][
                    platform
                ]["msecs"] = 0
            platform_buckets[bucket]["worker_types"][worker_type]["platforms"][
                platform
            ]["msecs"] += worker_type_durations[worker_type][platform]
            if worker_type in worker_types:
                platform_cost = (
                    worker_type_durations[worker_type][platform]
                    / worker_type_durations[worker_type]["total"]
                    * worker_types[worker_type]["cost"]
                )
                # print(bucket, worker_type, platform,
                #      worker_type_durations[worker_type][platform], ' / ',
                #      worker_type_durations[worker_type]['total'], ' x ',
                #      worker_types[worker_type]['cost'], ' = ',
                #      platform_cost)
            else:
                platform_cost = 0
            platform_buckets[bucket]["worker_types"][worker_type]["platforms"][
                platform
            ]["cost"] = platform_cost
            platform_buckets[bucket]["worker_types"][worker_type][
                "cost"
            ] += platform_cost
            platform_buckets[bucket]["bucket_cost"] += platform_cost

    header = [
        "Bucket",
        "Worker Type",
        "Platform" "Cost ($)",
        "Duration (msecs)",
        "Year",
        "Month",
    ]
    output = []
    print("Platforms sorted by cost")
    print("========================")
    for bucket in sorted(
        platform_buckets,
        key=lambda x: (platform_buckets[x]["bucket_cost"]),
        reverse=True,
    ):
        print(
            "{0:<25s} ${1:>15,.2f}".format(
                bucket + ":", float(platform_buckets[bucket]["bucket_cost"])
            )
        )
        for worker_type in sorted(
            platform_buckets[bucket]["worker_types"],
            key=lambda y: (platform_buckets[bucket]["worker_types"][y]["cost"]),
            reverse=True,
        ):
            print(
                "\t{0:<25s} ${1:>15,.2f} (platforms: {2})".format(
                    worker_type + ":",
                    float(
                        platform_buckets[bucket]["worker_types"][worker_type]["cost"]
                    ),
                    ", ".join(
                        [
                            p
                            for p in platform_buckets[bucket]["worker_types"][
                                worker_type
                            ]["platforms"]
                            if p != "total"
                        ]
                    ),
                )
            )
            for p in platform_buckets[bucket]["worker_types"][worker_type]["platforms"]:
                if p != "total":
                    output.append(
                        [
                            bucket,
                            worker_type,
                            p,
                            platform_buckets[bucket]["worker_types"][worker_type][
                                "platforms"
                            ][p]["cost"],
                            platform_buckets[bucket]["worker_types"][worker_type][
                                "platforms"
                            ][p]["msecs"],
                            year,
                            month,
                        ]
                    )

        print("")

    print("Platforms sorted by time")
    print("========================")
    for bucket in sorted(
        platform_buckets,
        key=lambda x: (platform_buckets[x]["bucket_msecs"]),
        reverse=True,
    ):
        print(
            "{0:<25s} {1:>15,.2f} hrs".format(
                bucket + ":",
                float(platform_buckets[bucket]["bucket_msecs"] / 1000 / 60 / 60),
            )
        )
        for worker_type in sorted(
            platform_buckets[bucket]["worker_types"],
            key=lambda y: (platform_buckets[bucket]["worker_types"][y]["msecs"]),
            reverse=True,
        ):
            print(
                "\t{0:<25s} {1:>15,.2f} hrs (platforms: {2})".format(
                    worker_type + ":",
                    float(
                        platform_buckets[bucket]["worker_types"][worker_type]["msecs"]
                        / 1000
                        / 60
                        / 60
                    ),
                    ", ".join(
                        [
                            p
                            for p in platform_buckets[bucket]["worker_types"][
                                worker_type
                            ]["platforms"]
                            if p != "total"
                        ]
                    ),
                )
            )
        print("")
    # pp.pprint(output)

    # Double-check cost per platform bucket
    # cost_check = 0
    # for worker_type in platform_buckets['windows7']['worker_types']:
    #     for platform in platform_buckets['windows7']['worker_types'][worker_type]['platforms']:
    #         cost_check += platform_buckets['windows7']['worker_types'][worker_type]['platforms'][platform]['cost']
    # print(platform_buckets['windows7']['bucket_cost'], cost_check)

    # pp.pprint(platform_buckets)
    csv_filename = os.path.join(
        DATA_DIR, "platform_costs_{}-{:0>2}.csv".format(year, month)
    )
    with open(csv_filename, "w") as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=",")
        csvwriter.writerows(output)

    if not os.path.exists(instance_types_file):
        with open(instance_types_file, "w") as outfile:
            json.dump(instance_types, outfile)
    if not os.path.exists(worker_types_file):
        with open(worker_types_file, "w") as outfile:
            json.dump(worker_types, outfile)
    if not os.path.exists(worker_type_durations_file):
        with open(worker_type_durations_file, "w") as outfile:
            json.dump(worker_type_durations, outfile)
