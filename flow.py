import os

import prefect
from prefect import Flow, Parameter
from prefect.engine.results import LocalResult
from prefect.engine.executors import LocalDaskExecutor, LocalExecutor
from prefect.utilities.edges import unmapped

from tasks import postgres, socrata


"""
Flow: Loading Socrata Data to Postgres
--------------------------------------
This flow downloads data from the Socrata Open Data site and loads it 
into a Postgres database.

Behavior is configured in the config.toml
"""

with Flow(
        'Loading Socrata data to Postgres',
        result=LocalResult(
            dir=os.path.join(os.path.dirname(__file__), "results"),
            validate_dir=True
            ),
        state_handlers=[postgres.log_to_database]
    ) as flow:
    
    datasets = Parameter("datasets")
    
    # get last updated from database
    since = postgres.get_last_updated()
    # download dataset from Socrata
    downloads = socrata.download_dataset.map(
        dataset=datasets,
        since=unmapped(since)
    )
    # get the temp tables ready for load
    prep = postgres.prep_load()
    # load each downloaded file
    load = postgres.load_datafile.map(
        datafile=downloads
    )
    # commit new data to database and clean up
    complete = postgres.complete_load()

    # make sure prep runs before load
    flow.add_edge(upstream_task=prep, downstream_task=load)
    # make sure load runs before complete
    flow.add_edge(upstream_task=load, downstream_task=complete)


if __name__ == "__main__":
    logger = prefect.context.get("logger")

    dask = prefect.config.dask
    mode = prefect.config.mode
    reset_db = prefect.config.reset_db

    all_datasets = dict(prefect.config.socrata.datasets)
    years = list(prefect.config.data.years)

    # use only year datasets if in full mode otherwise use all w/since
    if mode == 'full':
        run_datasets = dict((k, all_datasets[k]) for k in years)
    else:
        run_datasets = all_datasets

    logger.info(f"Starting \"{mode}\" flow for {', '.join(run_datasets.keys())} {'and resetting db' if reset_db else ''}")
    state = flow.run(
        datasets=list(run_datasets.values()),
        executor=LocalDaskExecutor() if dask else LocalExecutor()
    )
