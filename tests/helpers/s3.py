import boto3


def upload_test_file(local_file_path: str, bucket_name: str, target_file_name: str):
	client = __s3_client()
	client.upload_file(local_file_path, bucket_name, target_file_name)


def __s3_client():
	return boto3.client('s3')
