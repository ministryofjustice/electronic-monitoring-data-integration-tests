from pytest_bdd import scenario, given, when, then
from test_helpers.s3_helpers import upload_test_file



@given("I have uploaded the .bak files into s3")
def upload_dms_test_data_into_s3():
    upload_test_file(local_file_path="tests/test_data/dms_extraction/mock_test.txt", bucket_name="the-bucket", target_file_name="ConversationLog.bak")


@given("the test data has been ingested into RDS")
def ingest_test_data_into_rds():
    