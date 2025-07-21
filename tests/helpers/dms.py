import boto3
import logging
import time


def __dms_client():
    return boto3.client("dms")


def dms_task_waiter(func):
    def wrapper(*args, **kwargs):
        count = 0
        max_retries = 40
        while True:
            dms_task_response = func(*args, **kwargs)

            if not dms_task_response.get("ReplicationTasks"):
                raise ValueError("DMS task not found. Check the ARN and filters.")

            task_info = dms_task_response["ReplicationTasks"][0]
            status = task_info["Status"]

            logging.info(f"Retry {count}: Current Status: {status}")

            if status == "stopped":
                logging.info("DMS task has stopped")
                failure_value = task_info.get("LastFailureMessage", "")
                dms_progress = task_info.get("ReplicationTaskStats", {}).get(
                    "FullLoadProgressPercent", 0
                )

                if not failure_value and dms_progress == 100:
                    logging.info(
                        f"DMS Task has completed successfully. Progress: {dms_progress}%"
                    )
                    return dms_task_response  # Success, exit the loop
                else:
                    raise RuntimeError(
                        f"The DMS Task has Failed. Progress: {dms_progress}%. Error: {failure_value}"
                    )

            if status == "failed":
                failure_value = task_info.get("LastFailureMessage", "")
                raise RuntimeError(f"DMS task has failed: {failure_value}")

            if count >= max_retries:
                raise TimeoutError(
                    f"DMS Task did not complete within the allotted time: {max_retries * 10}(s)"
                )

            time.sleep(15)
            count += 1

    return wrapper


def start_dms_task_instance(dms_task_arn: str):
    client = __dms_client()
    return client.start_replication_task(
        ReplicationTaskArn=dms_task_arn, StartReplicationTaskType="reload-target"
    )


@dms_task_waiter
def check_dms_task_status(task_arn):
    client = __dms_client()
    return client.describe_replication_tasks(
        Filters=[
            {
                "Name": "replication-task-arn",
                "Values": [
                    task_arn,
                ],
            },
        ],
        Marker="string",
        WithoutSettings=False,
    )
