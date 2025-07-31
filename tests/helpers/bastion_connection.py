import subprocess
import os
import logging
import pytest
import time
import threading
from helpers.ec2 import retrieve_instance_id


@pytest.fixture
def create_ssh_tunnel_to_ec2_bastion():
	"""Pytest fixture to establish an SSH tunnel via an EC2 bastion host.

	It starts a subprocess for the tunnel, waits briefly for it to establish,
	and then yields control to the test. After the test completes, it
	terminates the tunnel subprocess.

	Yields:
	    None: This fixture does not return a value but manages the tunnel's lifecycle.

	Raises:
	    RuntimeError: If the subprocess command fails to start the tunnel.
	"""
	instance_id = retrieve_instance_id('bastion_linux')
	cmd = [
		'aws',
		'ssm',
		'start-session',
		'--region',
		RetrieveBastionCredentials.region,
		'--target',
		instance_id,
		'--document-name',
		'AWS-StartPortForwardingSessionToRemoteHost',
		'--parameters',
		f'host={RetrieveBastionCredentials.rds_host},portNumber={RetrieveBastionCredentials.port},localPortNumber={RetrieveBastionCredentials.port}',
	]
	try:
		logging.info('Launching subprocess to create SSM port forwarding tunnel.')
		tunnel = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		time.sleep(4)
		__logging_ssh_output(tunnel)
	except Exception as e:
		raise RuntimeError('Failed to create port forward tunnel') from e

	yield

	logging.info('Tearing down SSH tunnel.')
	tunnel.kill()


class RetrieveBastionCredentials:
	"""A configuration class to hold credentials for the bastion connection.

	Attributes:
	    region (str): The AWS region where the resources are located.
	    rds_host (str): The endpoint hostname of the target RDS instance.
	    port (str): The port number for both the RDS host and the local forward.
	"""

	region = os.environ['REGION']
	rds_host = os.environ['RDS_HOST']
	port = os.environ['PORT']


def __logging_ssh_output(tunnel: subprocess.Popen):
	"""Logs stdout and stderr from a subprocess in separate threads.

	Args:
	    tunnel (subprocess.Popen): The running subprocess object whose output
	        needs to be logged.
	"""

	def __stream_output(pipe):
		"""Reads and logs lines from a given pipe."""
		for line in iter(pipe.readline, b''):
			logging.info(line.decode().strip())

	threading.Thread(target=__stream_output, args=(tunnel.stdout), daemon=True).start()
	threading.Thread(target=__stream_output, args=(tunnel.stderr), daemon=True).start()
