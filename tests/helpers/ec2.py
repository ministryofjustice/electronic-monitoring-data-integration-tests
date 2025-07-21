import boto3


def retrieve_instance_id(instance_name: str) -> str:
	"""Retrieves the instance ID of an EC2 instance from its 'Name' tag.

	Args:
	    instance_name (str): The value of the 'Name' tag assigned to the EC2 instance.

	Returns:
	    str: The unique ID of the found EC2 instance (e.g., 'i-1234567890abcdef0').

	Raises:
	    IndexError: If no EC2 instance with the specified 'Name' tag is found.
	"""
	client = __client()
	ec2_instances = client.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [instance_name]}])

	instance_id = None
	for instance in ec2_instances:
		# This will get the last instance ID if multiple instances share the same name
		instance_id = instance.id

	if instance_id is None:
		raise IndexError(f'{instance_name} EC2 instance was not found.')

	return instance_id


def __client():
	"""Creates and returns a Boto3 EC2 resource object.

	This is a private helper function to initialize the high-level Boto3
	EC2 service resource.

	Returns:
	    boto3.resource: An EC2 service resource object.
	"""
	return boto3.resource('ec2')
