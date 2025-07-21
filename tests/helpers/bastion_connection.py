import subprocess
import os
import logging
import pytest
import time
import threading
from helpers.ec2 import retrieve_instance_id


@pytest.fixture
def create_ssh_tunnel_to_ec2_bastion():
    instance_id = retrieve_instance_id("bastion_linux")
    cmd = [
        "aws",
        "ssm",
        "start-session",
        "--region",
        RetrieveBastionCredentials.region,
        "--target",
        instance_id,
        "--document-name",
        "AWS-StartPortForwardingSessionToRemoteHost",
        "--parameters",
        f"host={RetrieveBastionCredentials.rds_host},portNumber={RetrieveBastionCredentials.port},localPortNumber={RetrieveBastionCredentials.port}",
    ]
    try:
        logging.info("Launching Subprocess \n")
        tunnel = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(4)
        __logging_ssh_output(tunnel)
    except Exception as e:
        raise RuntimeError("Failed to create port forward tunnel") from e

    yield
    tunnel.kill()


class RetrieveBastionCredentials:
    region = os.environ["REGION"]
    rds_host = os.environ["RDS_HOST"]
    port = os.environ["PORT"]


def __logging_ssh_output(tunnel):
    def __stream_output(pipe, level=logging.INFO):
        for line in iter(pipe.readline, b""):
            logging.info(line.decode().strip())

    threading.Thread(
        target=__stream_output, args=(tunnel.stdout, logging.INFO), daemon=True
    ).start()
    threading.Thread(
        target=__stream_output, args=(tunnel.stderr, logging.INFO), daemon=True
    ).start()
