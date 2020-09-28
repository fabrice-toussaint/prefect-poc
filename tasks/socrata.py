from datetime import timedelta, datetime
import csv
import os
from urllib import parse

from sodapy import Socrata

import prefect
from prefect.utilities.tasks import task

"""
This task will call Socrata and output the returned data in CSV format.

This uses the Sodapy Python client library to call the Socrata API:
    https://github.com/xmunoz/sodapy

More information about how to use SoQL (the Socrata query language) is here:
    https://dev.socrata.com/docs/queries/

"""


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def download_dataset(
        dataset,
        mode: str,
        fieldnames,
        domain: str,
        token: str,
        since: datetime = None,
        max_rows: int = 2000000,
        batch_size: int = 100000
    ):
   
    dataset_key = dataset

    # config for run 
    offset = 0
    if mode == "full":
        where = None
    else:
        where = None if since is None else f"updateddate > '{datetime.strptime(since, '%Y-%m-%dT%H:%M:%S').isoformat()}'"
    output_file = f"output/{prefect.context.task_name}-{mode}-{dataset_key}-{prefect.context.today}.csv"
    logger = prefect.context.get("logger")

    # create Socrata client
    client = Socrata(
        domain,
        token
    )

    # start downloading Socrata dataset in batches
    logger.info(f"Downloading dataset: {dataset_key}")

    while offset < max_rows:
        limit = min(batch_size, max_rows - offset)

        logger.info(f'Fetching {limit} rows with offset {offset}')
        
        rows = client.get(
            dataset_key,
            select=",".join(fieldnames),
            limit=limit,
            offset=offset,
            where=where
        )

        if len(rows) > 0:

            if offset == 0:
                # prepare output CSV
                os.makedirs(os.path.dirname(output_file), exist_ok=True)

                with open(output_file, "w") as fd:
                    writer = csv.DictWriter(fd, fieldnames)
                    writer.writeheader()

            logger.info(f'Adding {len(rows)} rows')

            # append batch to CSV file
            with open(output_file, "a") as fd:
                writer = csv.DictWriter(fd, fieldnames)   
                writer.writerows(rows)

            offset += len(rows)

        if len(rows) < batch_size:
            break

    # wrap up
    logger.info(f'{offset} total rows downloaded for dataset: {dataset_key}')
