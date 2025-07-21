import os
import logging
import pytest

if os.getenv("CI") is None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.info("Local Run")


pytest_plugins = ["helpers.bastion_connection"]


@pytest.fixture()
def tasks_sql():
    return """exec msdb.dbo.rds_task_status @task_id={task_id:}"""


@pytest.fixture()
def restore_sql():
    return """exec msdb.dbo.rds_restore_database
        @restore_db_name='testing_regression',
        @s3_arn_to_restore_from='{arn:}',
        @with_norecovery=0,
        @type='FULL';"""
