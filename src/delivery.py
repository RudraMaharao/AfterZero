"""DynamoDB stream outbox dispatch + Step Functions evidence/inbox tasks."""
import hashlib
import json
import os
import time

import boto3
from botocore.exceptions import ClientError
from core import canonical


def dispatch(event, context):
    client = boto3.client("stepfunctions")
    failures = []
    for record in event.get("Records", []):
        if record["eventName"] == "REMOVE":
            continue
        item = record.get("dynamodb", {}).get("NewImage", {})
        if "effect" not in item:
            continue
        old = record.get("dynamodb", {}).get("OldImage", {}).get("effect")
        if old == item["effect"]:
            continue
        value = item["effect"]["S"]
        name = "az-" + hashlib.sha256(value.encode()).hexdigest()
        try:
            client.start_execution(stateMachineArn=os.environ["STATE_MACHINE_ARN"], name=name, input=value)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ExecutionAlreadyExists":
                failures.append({"itemIdentifier": record["dynamodb"]["SequenceNumber"]})
    return {"batchItemFailures": failures}


def archive(event, context):
    raw = canonical(event).encode()
    sha = hashlib.sha256(raw).hexdigest()
    key = f"evidence/{event['tenant']}/{event['scope_id']}/{event['version']:06d}.json"
    boto3.client("s3").put_object(Bucket=os.environ["BUCKET_NAME"], Key=key,
                                 Body=canonical({"sha256": sha, "snapshot": event}).encode(), ContentType="application/json")
    return {**event, "evidence_key": key, "sha256": sha}


def publish(event, context):
    table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    mid = f"INBOX#{event['scope_id']}#{event['version']:06d}"
    try:
        table.put_item(Item={"pk": event["tenant"], "sk": mid, "message": canonical(event),
                            "acknowledged": False, "ttl": int(time.time()) + 604800},
                       ConditionExpression="attribute_not_exists(pk)")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    return {"delivered": True, "message_id": mid, "evidence_key": event["evidence_key"], "sha256": event["sha256"]}
