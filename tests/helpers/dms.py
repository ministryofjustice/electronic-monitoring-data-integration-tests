import boto3
import logging
import time
from typing import Callable


def dms_task_waiter(func: Callable) -> Callable:
	"""A decorator that polls a DMS task's status until it completes or fails.

	This waiter wraps a function that describes a DMS task. It repeatedly calls
	the function, checking the task's status in a loop. It handles success,
	failure, and timeout conditions.

	Args:
	    func (Callable): The function to decorate. It's expected to return
	        a dictionary - `describe_replication_tasks`.

	Returns:
	    Callable: The wrapper function which, when called, will wait until
	        the DMS task reaches a terminal state.

	Raises:
	    ValueError: If the decorated function's response doesn't contain
	        the expected task information.
	    RuntimeError: If the DMS task stops with an error or enters the
	        'failed' state.
	    TimeoutError: If the task doesn't complete within the allotted time.
	"""

	def wrapper(*args, **kwargs):
		count = 0
		max_retries = 40
		while True:
			dms_task_response = func(*args, **kwargs)

			if not dms_task_response.get('ReplicationTasks'):
				raise ValueError('DMS task not found. Check the ARN and filters.')

			task_info = dms_task_response['ReplicationTasks'][0]
			status = task_info['Status']

			logging.info(f'Retry {count}: Current Status: {status}')

			if status == 'stopped':
				logging.info('DMS task has stopped')
				failure_value = task_info.get('LastFailureMessage', '')
				dms_progress = task_info.get('ReplicationTaskStats', {}).get('FullLoadProgressPercent', 0)

				if not failure_value and dms_progress == 100:
					logging.info(f'DMS Task has completed successfully. Progress: {dms_progress}%')
					return dms_task_response  # Success, exit the loop
				else:
					raise RuntimeError(f'The DMS Task has Failed. Progress: {dms_progress}%. Error: {failure_value}')

			if status == 'failed':
				failure_value = task_info.get('LastFailureMessage', '')
				raise RuntimeError(f'DMS task has failed: {failure_value}')

			if count >= max_retries:
				raise TimeoutError(f'DMS Task did not complete within the allotted time: {max_retries * 15}(s)')

			time.sleep(15)
			count += 1

	return wrapper


def start_dms_task_instance(dms_task_arn: str) -> dict:
	"""Starts a DMS replication task in 'reload-target' mode.

	Args:
	    dms_task_arn (str): The Amazon Resource Name (ARN) of the DMS
	        replication task to start.

	Returns:
	    dict: The response from the Boto3 `start_replication_task` API call.
	"""
	client = __dms_client()
	return client.start_replication_task(ReplicationTaskArn=dms_task_arn, StartReplicationTaskType='reload-target')


@dms_task_waiter
def check_dms_task_status(task_arn: str) -> dict:
	"""Checks the status of a DMS task and waits for it to complete.

	NOTE: This function is decorated by `dms_task_waiter`. Calling it will
	initiate a polling loop that repeatedly checks the task's status until it
	stops successfully, fails, or times out.

	Args:
	    task_arn (str): The ARN of the DMS task to monitor.

	Returns:
	    dict: The final successful response from the `describe_replication_tasks`
	        API call once the task is complete.
	"""
	client = __dms_client()
	return client.describe_replication_tasks(
		Filters=[
			{
				'Name': 'replication-task-arn',
				'Values': [
					task_arn,
				],
			},
		],
		WithoutSettings=False,
	)


def __dms_client():
	"""Creates and returns a Boto3 DMS client.

	Private helper function to initialize the low-level AWS DMS client.

	Returns:
	    boto3.client: An AWS DMS client object.
	"""
	return boto3.client('dms')
