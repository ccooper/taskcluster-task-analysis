#!/usr/bin/env python

import csv
import os
import re
import sys

from datetime import datetime

worker_types = []
values = []


def get_month_year_from_filename(filepath):
    # filename e.g. worker_type_hours_cost_jul2018.csv
    filename = os.path.basename(filepath)
    try:
        parsed_date = datetime.strptime(filename, "worker_type_hours_cost_%B%Y.csv")
        return parsed_date.month, parsed_date.year
    except ValueError:
        sys.exit("Unable to parse month from filepath: %s" % filepath)


def extract_provisioner_from_worker_type(worker_type):
    data = worker_type.split("/", 2)
    if data:
        if len(data) > 1:
            return data
        return ["none", data[0]]
    raise ValueError


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit("Please pass in the path of the csv file to parse.")
    filename = sys.argv[1]

    parsed_month, parsed_year = get_month_year_from_filename(filename)

    with open(filename, "rb") as csvfile:
        cost_data = csv.reader(csvfile, delimiter=",")
        rownum = 0
        colnum = 0

        for row in cost_data:
            for entry in row:
                if rownum == 0:
                    worker_types.append(entry)
                else:
                    values.append(entry)
                colnum += 1
            rownum += 1
            # The default csv download for a month has two rows: one for total
            # and one for the month. This is redundant in the single month case
            # so we just read the first data row.
            if rownum > 1:
                break

    worker_type_costs = {}

    for i in range(len(worker_types)):
        if i == 0:
            continue
        # Figure out what the worker type is, and whether the value represents cost or hours
        m = re.search("^(.*)\s*\(Hrs\)", worker_types[i])
        if m:
            provisioner, worker_type = extract_provisioner_from_worker_type(m.group(1))
            if re.search("Total usage", worker_type):
                worker_type = "Total"
            if provisioner not in worker_type_costs:
                worker_type_costs[provisioner] = {}
            if worker_type not in worker_type_costs[provisioner]:
                worker_type_costs[provisioner][worker_type] = {}
            worker_type_costs[provisioner][worker_type]["hours"] = values[i]
            continue
        m = re.search("^(.*)\(\$\)", worker_types[i])
        if m:
            provisioner, worker_type = extract_provisioner_from_worker_type(m.group(1))
            if re.search("Total cost", worker_type):
                worker_type = "Total"
            if provisioner not in worker_type_costs:
                worker_type_costs[provisioner] = {}
            if worker_type not in worker_type_costs[provisioner]:
                worker_type_costs[provisioner][worker_type] = {}
            worker_type_costs[provisioner][worker_type]["cost"] = values[i]
            continue

    for provisioner in worker_type_costs:
        for worker_type in worker_type_costs[provisioner]:
            coopthing = worker_type_costs[provisioner][worker_type]
            if "hours" in coopthing and "cost" in coopthing:
                print(
                    "INSERT INTO worker_type_monthly_costs \
                        (year, month, provider, provisioner, worker_type, usage_hours, cost) \
                        VALUES (%d, %d, 'aws', '%s', '%s', %.2f, %.2f);"
                    % (
                        parsed_year,
                        parsed_month,
                        provisioner,
                        worker_type,
                        float(coopthing["hours"]),
                        float(coopthing["cost"]),
                    )
                )
            else:
                print(worker_type + " missing an expected key")
