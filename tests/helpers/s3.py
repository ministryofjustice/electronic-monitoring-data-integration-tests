import boto3
import pandas as pd
import io


def upload_test_file(local_file_path: str, bucket_name: str, target_file_name: str):
	"""Uploads a local file to a specified S3 bucket.

	Args:
	    local_file_path (str): The path to the local file to upload.
	    bucket_name (str): The name of the target S3 bucket.
	    target_file_name (str): The desired name (key) for the file in the bucket.
	"""
	client = __s3_client()
	client.upload_file(local_file_path, bucket_name, target_file_name)


def download_file(bucket: str, key: str) -> pd.DataFrame:
	"""Downloads file and returns a pandas dataframe.

	Args:
		bucket (str): Target Bucket
		key (str): file key to read

	Returns:
		pd.DataFrame: Pandas Dataframe
	"""

	client = __s3_client()
	file = client.get_object(Bucket=bucket, Key=key)
	return pd.read_parquet(io.BytesIO(file['Body'].read()))


def __s3_client():
	"""Creates and returns a boto3 S3 client.

	This is a private helper function to initialize and provide a low-level
	S3 client object.

	Returns:
	    boto3.client: An S3 client object.
	"""
	return boto3.client('s3')
