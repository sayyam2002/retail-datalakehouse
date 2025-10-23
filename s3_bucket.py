import boto3
from botocore.exceptions import ClientError
import os
import re

# ===== CONFIG =====
region = 'us-east-1'
bucket_list = [
    'apex-retail-raw-zone',
    'apex-retail-bronze-zone',
    'apex-retail-silver-zone',
    'apex-retail-gold-zone'
]

# Local dataset folder (update this path as per your system)
local_raw_path = r"./datasets"   # ✅ your dataset folder in project directory
raw_bucket = bucket_list[0]     # apex-retail-raw-zone

# ===== INITIALIZE CLIENT =====
s3_client = boto3.client('s3', region_name=region)

# ===== CREATE BUCKETS =====
for bucket in bucket_list:
    try:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket)
        else:
            s3_client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"✅ Bucket created: {bucket}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"ℹ Bucket already exists and owned by you: {bucket}")
        else:
            print(f"❌ Error creating bucket {bucket}: {e}")

# ===== UPLOAD FILES TO RAW BUCKET =====
if not os.path.exists(local_raw_path):
    print(f"❌ Dataset path not found: {local_raw_path}")
else:
    for file_name in os.listdir(local_raw_path):
        if not file_name.endswith(".csv"):
            continue

        # Determine S3 subpath based on filename
        file_path = os.path.join(local_raw_path, file_name)

        if file_name.startswith("orders"):
            match = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
            date_folder = match.group(1) if match else "unknown_date"
            s3_key = f"transactional/orders/{date_folder}/{file_name}"

        elif file_name.startswith("order_items"):
            match = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
            date_folder = match.group(1) if match else "unknown_date"
            s3_key = f"transactional/order_items/{date_folder}/{file_name}"

        elif file_name.startswith("products"):
            s3_key = f"product/{file_name}"

        else:
            s3_key = f"misc/{file_name}"

        try:
            s3_client.upload_file(file_path, raw_bucket, s3_key)
            print(f"✅ Uploaded: {file_name} → s3://{raw_bucket}/{s3_key}")
        except Exception as e:
            print(f"❌ Failed to upload {file_name}: {e}")

print("\n🎉 All raw datasets uploaded successfully to S3!")