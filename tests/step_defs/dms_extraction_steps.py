from helpers.s3 import upload_test_file, download_file, dynamically_retrieve_bucket
from helpers.rds_connection import ConnectToRDS
from helpers.dms import start_dms_task_instance, check_dms_task_status
import os
import time
import logging
import pandas as pd
from pytest_bdd import scenarios, given, when, then
import datacompy


scenarios('../features/dms_extraction.feature')


@given('I have an active AWS SSH Tunnel')
def activate_ssh_tunnel(create_ssh_tunnel_to_ec2_bastion):
	pass


@given('I have uploaded the .bak files into s3')
def upload_dms_test_data_into_s3(context):
	emds_data_bucket = dynamically_retrieve_bucket(search_string=f'emds-{os.environ["ENV"]}-data')
	context['emds-data-bucket'] = emds_data_bucket
	upload_test_file(
		local_file_path='tests/test_data/dms_extraction/BackupFile.bak',
		bucket_name=emds_data_bucket,
		target_file_name='BackupFile.bak',
	)


@given('the test data has been ingested into RDS')
def ingest_test_data_into_rds(restore_sql, tasks_sql, context):
	server_endpoint = f'{os.environ["HOST_NAME"]},{os.environ["PORT"]}'
	rds_connection = ConnectToRDS(
		user=os.environ['USER_NAME'],
		password=os.environ['PASSWORD'],
		server_endpoint=server_endpoint,
	)
	arn = f'arn:aws:s3:::{context["emds-data-bucket"]}/BackupFile.bak'
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
			raise TimeoutError(f'Failed to restore backup within adequate time: {count * 15}(s)')

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
def validate_dms_output(context, delete_test_data):
	extract_bucket = dynamically_retrieve_bucket(search_string=f'emds-{os.environ["ENV"]}-dms-rds-to-parquet')
	context['extract-bucket'] = extract_bucket
	context['dms-prefix'] = 'test'

	output_order_description = download_file(bucket=extract_bucket, key='test/dbo/d_order_description/LOAD00000001.parquet')
	expected_order_description = pd.read_parquet('tests/test_data/dms_extraction/expected/d_order_description.parquet')
	_datacompy_compare(
		expected=expected_order_description,
		output=output_order_description,
		join_col='order_description_sid',
		dataset_name='d_order_description',
	)

	output_d_subject = download_file(bucket=extract_bucket, key='test/dbo/d_subject/LOAD00000001.parquet')
	expected_d_subject = pd.read_parquet('tests/test_data/dms_extraction/expected/d_subject.parquet')
	_datacompy_compare(expected=expected_d_subject, output=output_d_subject, join_col='prison_no', dataset_name='d_subject')

	output_device = download_file(bucket=extract_bucket, key='test/dbo/device/LOAD00000001.parquet')
	expected_device = pd.read_parquet('tests/test_data/dms_extraction/expected/device.parquet')
	_datacompy_compare(expected=expected_device, output=output_device, join_col='device_id', dataset_name='device')

	output_f_order = download_file(bucket=extract_bucket, key='test/dbo/f_order/LOAD00000001.parquet')
	expected_f_order = pd.read_parquet('tests/test_data/dms_extraction/expected/f_order.parquet')
	_datacompy_compare(expected=expected_f_order, output=output_f_order, join_col='address_sid', dataset_name='f_order')

	output_gps_position = download_file(bucket=extract_bucket, key='test/dbo/gps_position/LOAD00000001.parquet')
	expected_gps_position = pd.read_parquet('tests/test_data/dms_extraction/expected/gps_position.parquet')
	_datacompy_compare(
		expected=expected_gps_position, output=output_gps_position, join_col='person_id', dataset_name='gps_position'
	)


def _datacompy_compare(expected, output, join_col, dataset_name):
	compare = datacompy.Compare(
		expected, output, join_columns=join_col, df1_name=f'{dataset_name}-Expected', df2_name=f'{dataset_name}-Output'
	)
	compare.matches(ignore_extra_columns=False)
	with open(f'{dataset_name}-report.txt', 'w') as f:
		f.write(compare.report())
