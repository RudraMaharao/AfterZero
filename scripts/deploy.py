"""Manual SAM/CloudFormation deployment; no build runner or deployment pipeline."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import time
import zipfile

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
REGION = os.environ.get("AWS_REGION", "ap-south-1")
STACK = os.environ.get("AFTERZERO_STACK", "afterzero")
session = boto3.Session(region_name=REGION)
s3, cf = session.client("s3"), session.client("cloudformation")
account = session.client("sts").get_caller_identity()["Account"]
bootstrap = f"{STACK}-artifacts-{account}-{REGION}"
data_bucket = f"{STACK}-data-{account}-{REGION}"
out = ROOT / ".deploy"
out.mkdir(exist_ok=True)
try:
    s3.head_bucket(Bucket=bootstrap)
except ClientError as e:
    if e.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
        raise
    options = {} if REGION == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": REGION}}
    s3.create_bucket(Bucket=bootstrap, **options)
    s3.put_public_access_block(Bucket=bootstrap, PublicAccessBlockConfiguration={"BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_encryption(Bucket=bootstrap, ServerSideEncryptionConfiguration={"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
zip_path = out / "function.zip"
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
    for p in sorted((ROOT / "src").glob("*.py")):
        z.write(p, p.name)
    for p in sorted((ROOT / "web").iterdir()):
        if p.is_file():
            z.write(p, "web/" + p.name)
key = "releases/" + hashlib.sha256(zip_path.read_bytes()).hexdigest() + ".zip"
s3.upload_file(str(zip_path), bootstrap, key)
secret_file = out / "admin-token"
if not secret_file.exists():
    secret_file.write_text(secrets.token_urlsafe(40))
    secret_file.chmod(0o600)
params = [{"ParameterKey": k, "ParameterValue": v} for k, v in {
    "ArtifactsBucket": bootstrap, "CodeKey": key, "DataBucketName": data_bucket,
    "AdminToken": secret_file.read_text().strip()}.items()]
kwargs = {"StackName": STACK, "TemplateBody": (ROOT / "template.yaml").read_text(), "Parameters": params,
          "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND"], "Tags": [{"Key": "Project", "Value": "AfterZero"}]}
try:
    cf.describe_stacks(StackName=STACK)
    update = True
except ClientError as e:
    if "does not exist" not in str(e):
        raise
    update = False
try:
    (cf.update_stack if update else cf.create_stack)(**kwargs)
except ClientError as e:
    if "No updates are to be performed" not in str(e):
        raise
print("Deployment started:", STACK, REGION, flush=True)
while True:
    stack = cf.describe_stacks(StackName=STACK)["Stacks"][0]
    status = stack["StackStatus"]
    print(status, flush=True)
    if status in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
        break
    if "IN_PROGRESS" not in status:
        events = cf.describe_stack_events(StackName=STACK)["StackEvents"]
        for e in events:
            if "FAILED" in e["ResourceStatus"]:
                print(e["LogicalResourceId"], e.get("ResourceStatusReason"), flush=True)
        raise SystemExit("Deployment failed: " + status)
    time.sleep(10)
outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}
outputs.update(region=REGION, stack=STACK)
(out / "outputs.json").write_text(json.dumps(outputs, indent=2))
print(json.dumps(outputs, indent=2))
