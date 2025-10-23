import boto3
import json
from botocore.exceptions import ClientError

# Initialize IAM client
iam = boto3.client('iam')

# Define your bucket
bucket_name = "s3-raw-zone-retail"

# Define the policy JSON
policy_name = "S3-bucket-custom-policy"
policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": f"arn:aws:s3:::{bucket_name}"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": f"arn:aws:s3:::{bucket_name}/*"
        }
    ]
}

try:
    response = iam.create_policy(
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_document),
        Description=f"Allows read/write access to the {bucket_name} bucket"
    )
    print(f" Policy created successfully: {response['Policy']['Arn']}")
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityAlreadyExists':
        print(f" Policy '{policy_name}' already exists.")
    else:
        print(f" Error creating policy: {e}")

