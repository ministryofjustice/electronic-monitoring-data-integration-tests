from pytest_bdd import scenarios, given, when, then
from test_helpers.s3 import upload_test_file
from test_helpers.rds_connection import ConnectToRDS
import os

scenarios("../features/dms_extraction.feature")

@given("I have an active AWS SSH Tunnel")
def activate_ssh_tunnel(create_ssh_tunnel_to_ec2_bastion):
    pass

@given("I have uploaded the .bak files into s3")
def upload_dms_test_data_into_s3():
    pass
    # upload_test_file(local_file_path="tests/test_data/dms_extraction/mock_test.txt", bucket_name="the-bucket", target_file_name="ConversationLog.bak")


@given("the test data has been ingested into RDS")
def ingest_test_data_into_rds():
    rds_connection = ConnectToRDS(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host_name=os.environ["HOST_NAME"],
        port=os.environ["PORT"]
    )
