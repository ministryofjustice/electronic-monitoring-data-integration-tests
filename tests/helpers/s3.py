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


def dynamically_retrieve_bucket(search_string: str) -> str:
	"""Finds a single S3 bucket by a substring.

	Args:
	    search_string (str): The substring to search for within S3 bucket names.

	Returns:
	    str: The name of the unique bucket found.

	Raises:
	    LookupError: If no bucket name contains the search_string.
	    ValueError: If multiple bucket names contain the search_string.
	"""

	all_buckets = boto3.client('s3').list_buckets()['Buckets']
	matching_buckets = [bucket['Name'] for bucket in all_buckets if search_string in bucket['Name']]

	if not matching_buckets:
		raise LookupError(f"No Bucket found containing '{search_string}'")
	elif len(matching_buckets) > 1:
		raise ValueError(f"Multiple buckets found containing '{search_string}': {matching_buckets}")
	else:
		return matching_buckets[0]


def __s3_client():
	"""Creates and returns a boto3 S3 client.

	This is a private helper function to initialize and provide a low-level
	S3 client object.

	Returns:
	    boto3.client: An S3 client object.
	"""
	return boto3.client('s3')
