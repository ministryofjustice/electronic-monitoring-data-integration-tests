import boto3
import logging


def retrieve_instance_id(instance_name: str):
    client = __client()
    ec2_instances = client.instances.filter(
        Filters=[{"Name": "tag:Name", "Values": [instance_name]}]
    )

    logging.info(f"Type - {ec2_instances}")

    for instance in ec2_instances:
        instance_id = instance.id

    if instance_id is None:
        raise IndexError(f"{instance_name} EC2 instance was not found.")

    return instance_id


def __client():
    return boto3.resource("ec2")
