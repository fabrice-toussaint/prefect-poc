import os

from prefect import Flow, Parameter
from tasks import generate_rgbd
from datetime import timedelta
from prefect.schedules import IntervalSchedule

schedule = IntervalSchedule(interval=timedelta(hours=24))

with Flow(
        'Prefect Omdena POC',
        schedule
) as flow:
    args = {'artifact': 'pc_1583742419-7h4mpmc587_1591451354739_100_000.pcd',
            'base_data_path': '/usr/local/airflow/dags/data'
            }

    generate_rgbd.fuse_into_rgbd(artifact=args["artifact"], base_data_path=args["base_data_path"])

flow.run()
