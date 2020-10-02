import os
from os.path import join, dirname
from typing import List

import psycopg2
import prefect
from prefect.utilities.tasks import task


DATA_FOLDER = join(dirname(dirname(__file__)), 'output')
TEMP_TABLE = "temp_loading"

def infer_types(fields):
    return_fields = {}
    for key in fields.keys():
        if fields[key] == '':
            if key[-4:] == 'date':
                return_fields[key] = "timestamp without time zone"
            elif key in {'latitude', 'longitude'}:
                return_fields[key] = "double precision"
            else:
                return_fields[key] = "character varying"
        else:
            return_fields[key] = fields[key]

    return return_fields


@task
def get_last_updated():
    logger = prefect.context.get("logger")

    dsn = prefect.context.secrets["DSN"]
    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    # get last updated
    query = "select max(updateddate) from requests"
    cursor.execute(query)
    last_updated = cursor.fetchone()[0]
    connection.commit()

    cursor.close()
    connection.close()

    logger.info(last_updated)
    return last_updated


@task
def prep_load():
    logger = prefect.context.get("logger")

    dsn = prefect.context.secrets["DSN"]
    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()
    logger.info("Database connection established")

    fields = infer_types(prefect.config.data.fields)
    db_reset = prefect.config.reset_db
    target = prefect.config.data.target

    query = f"""
        CREATE TABLE IF NOT EXISTS {TEMP_TABLE} (
            {', '.join([f"{field} {fields[field]}" for field in fields])}
        );
    """
    cursor.execute(query)
    cursor.execute(f"TRUNCATE TABLE {TEMP_TABLE}")
    logger.info(f"'{TEMP_TABLE}' table truncated")

    if db_reset:
        cursor.execute(f"TRUNCATE TABLE {target}")
        logger.info(f"'{target}' table truncated")

    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def load_data(datasets: List[str]):
    logger = prefect.context.get("logger")
    mode = prefect.config.mode
    dsn = prefect.context.secrets["DSN"]

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    list_of_files = []
    for item in datasets:
        if mode == "full":
            file = join(DATA_FOLDER, f"{item}-{mode}.csv")
        else:
            file = join(DATA_FOLDER, f"{item}-{mode}-{prefect.context.today}.csv")

        if os.path.isfile(file):
            list_of_files.append(file)

    for file in list_of_files:
        
        with open(join(DATA_FOLDER, file), 'r') as f:
            try:
                cursor.copy_expert(
                    f"COPY {TEMP_TABLE} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    f
                )
                logger.info(f"Table '{TEMP_TABLE}' successfully loaded from {os.path.basename(file)}")
            except (Exception, psycopg2.DatabaseError) as error:
                logger.info("Error: %s" % error)
                connection.rollback()
                cursor.close()

    connection.commit()

    cursor.close()
    connection.close()
    logger.info("Database connection closed")


@task
def complete_load():
    logger = prefect.context.get("logger")
    dsn = prefect.context.secrets["DSN"]

    mode = prefect.config.mode
    db_reset = prefect.config.reset_db
    fieldnames = list(prefect.config.data.fields.keys())
    key = prefect.config.data.key
    target = prefect.config.data.target

    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()
    rows_inserted = 0 
    rows_updated = 0

    insert_query = f"""
        -- BEGIN;

        WITH rows AS (
            INSERT INTO {target} (
                {', '.join([f"{field}" for field in fieldnames])}
            )
            SELECT *
            FROM {TEMP_TABLE}
            ON CONFLICT ({key}) 
            DO NOTHING
            RETURNING 1
        )
        SELECT count(*) FROM rows;

        -- COMMIT;
    """

    update_query = f"""
        WITH rows AS (
            UPDATE {target}
            SET
                {', '.join([f"{field} = source.{field}" for field in fieldnames])}
            FROM (SELECT * FROM {TEMP_TABLE}) AS source
            WHERE {target}.{key} = source.{key}
            RETURNING 1
        )
        SELECT count(*) FROM rows;    
    """

    # TODO make generic/configurable
    refresh_view_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY service_requests;
    """

    # count rows to be upserted
    cursor.execute(f"SELECT COUNT(*) FROM {TEMP_TABLE}")
    rows_to_upsert = cursor.fetchone()[0]
    logger.info(f"Insert/updating '{target}' table with {rows_to_upsert:,} new records")
    
    # insert rows
    cursor.execute(insert_query)
    rows_inserted = cursor.fetchone()[0]
    connection.commit()
    logger.info(f"{rows_inserted:,} rows inserted in table '{target}'")

    # update rows if necessary
    if db_reset is False or mode == "diff":
        cursor.execute(update_query)
        rows_updated = cursor.fetchone()[0]
        connection.commit()
        logger.info(f"{rows_updated:,} rows updated in table '{target}'")

    # empty temp table if resetting the db
    if db_reset:
        cursor.execute(f"TRUNCATE TABLE {TEMP_TABLE}")    

    cursor.execute(refresh_view_query)
    # TODO make generic/configurable
    cursor.execute("UPDATE metadata SET last_pulled = NOW()")
    connection.commit()
    logger.info("Views successfully refreshed")

    # need to have autocommit set for VACUUM to work
    connection.autocommit = True
    cursor.execute("VACUUM FULL ANALYZE")
    logger.info("Database vacuumed and analyzed")

    cursor.close()
    connection.close()
    logger.info("Database connection closed")


def log_to_database(task, old_state, new_state):
    if new_state.is_finished():
        msg = "{0} finished with message \"{1}\"".format(task.name, new_state.message)

        if new_state.is_successful():
            status = "INFO"
        elif new_state.is_failed():
            status = "ERROR"
        else:
            status = "WARN"

        # log task results
        logger = prefect.context.get("logger")
        logger.info(msg)

        # write task results to database
        dsn = prefect.context.secrets["DSN"]
        connection = psycopg2.connect(dsn)
        cursor = connection.cursor()

        insert_query = f"""
            INSERT INTO log (status, message)
            VALUES ('{status}', '{msg}')
        """
        cursor.execute(insert_query)
        connection.commit()
        cursor.close()
        connection.close()

        return new_state
