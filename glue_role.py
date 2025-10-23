import boto3
import json
from botocore.exceptions import ClientError

# AWS region
region = "us-east-1"
iam = boto3.client("iam", region_name=region)

# Role details
role_name = "AWSGlueServiceRole"

# Trust policy for AWS Glue
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}

# Permissions policies to attach
managed_policies = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess"
]


def create_glue_role():
    try:
        # Check if the role already exists
        iam.get_role(RoleName=role_name)
        print(f"ℹ️ Role already exists: {role_name}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            print(f"❌ Unexpected error checking role: {e}")
            return

    # Create the new IAM role
    try:
        print("🛠️ Creating IAM role for AWS Glue...")
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="IAM Role for AWS Glue Crawlers and Jobs",
            MaxSessionDuration=3600
        )
        print(f"✅ Created role: {role_name}")

        # Attach managed policies
        for policy_arn in managed_policies:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            print(f"🔗 Attached policy: {policy_arn}")

        print("\n✅ AWSGlueServiceRole setup complete and ready to use.")
    except ClientError as e:
        print(f"❌ Error creating or attaching policies: {e}")


if __name__ == "__main__":
    create_glue_role()
