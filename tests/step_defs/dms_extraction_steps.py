from pytest_bdd import scenarios, given, when, then
from helpers.s3 import upload_test_file, download_file
from helpers.rds_connection import ConnectToRDS
from helpers.dms import start_dms_task_instance, check_dms_task_status
import os
import time
import logging
import pandas as pd
from pandas.testing import assert_frame_equal


scenarios('../features/dms_extraction.feature')


@given('I have an active AWS SSH Tunnel')
def activate_ssh_tunnel(create_ssh_tunnel_to_ec2_bastion):
	pass


@given('I have uploaded the .bak files into s3')
def upload_dms_test_data_into_s3():
	bucket = f'emds-{os.environ["ENV"]}-data-20240917144025201600000001'
	upload_test_file(
		local_file_path='tests/test_data/dms_extraction/TestDatabase.bak',
		bucket_name=bucket,
		target_file_name='TestDatabase_b.bak',
	)


@given('the test data has been ingested into RDS')
def ingest_test_data_into_rds(restore_sql, tasks_sql):
	server_endpoint = f'{os.environ["HOST_NAME"]},{os.environ["PORT"]}'
	rds_connection = ConnectToRDS(
		user=os.environ['USER_NAME'],
		password=os.environ['PASSWORD'],
		server_endpoint=server_endpoint,
	)
	arn = f'arn:aws:s3:::emds-{os.environ["ENV"]}-data-20240917144025201600000001/TestDatabase_b.bak'
	task_id = rds_connection.read_query(restore_sql.format(arn=arn))[0][0]

	count = 0
	backup_task_status = rds_connection.read_query(tasks_sql.format(task_id=task_id))[0][5]
	while True:
		if backup_task_status == 'SUCCESS':
			logging.info(f'Backup has been succesful - Task_id: {task_id}')
			break

		if backup_task_status == 'FAILED':
			raise RuntimeError(f'Failed to restore backup on RDS - Task_id: {task_id}')

		if count >= 15:
			raise TimeoutError(f'Failed to restore backup within adequate time: {count * 5}(s)')

		time.sleep(15)
		count += 1

		backup_task_status = rds_connection.read_query(tasks_sql.format(task_id=task_id))[0][5]

		if count >= 15:
			raise TimeoutError(f'Failed to restore backup within adequate time: {count * 5}(s)')

		logging.info(f'RDS still performing backup - current status {backup_task_status}')


@when('I await for the for DMS task to be complete')
def trigger_dms_instance():
	dms_task_arn = f'arn:aws:dms:{os.environ["REGION"]}:{os.environ["ACCOUNT_ID"]}:task:XMM5S2SB4BAPNMRHMRKDJNDFZE'
	start_dms_task_instance(dms_task_arn)
	check_dms_task_status(dms_task_arn)


@then('I validate the DMS extraction')
def validate_dms_output():
	local_file = 'path'
	s3_dataframe = download_file(bucket='Bucket', key='key')
	local_dataframe = pd.read_parquet(local_file)

	s3_dataframe = s3_dataframe.reindex(sorted(s3_dataframe.columns), axis=1)
	local_dataframe = local_dataframe.reindex(sorted(local_dataframe.columns), axis=1)

	assert_frame_equal(local_dataframe, s3_dataframe)
