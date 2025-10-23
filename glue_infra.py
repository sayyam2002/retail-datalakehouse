import boto3
import json
import time
from botocore.exceptions import ClientError

# ==============================
# CONFIGURATION
# ==============================
REGION = "us-east-1"
ROLE_NAME = "AWSGlueServiceRole"
DATABASE_NAME = "retail_lakehouse_db"
RAW_BUCKET = "apex-retail-raw-zone"

# Boto3 clients
iam = boto3.client("iam", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)

# ==============================
# STEP 1: CREATE IAM ROLE FOR GLUE
# ==============================
def create_glue_role():
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

    managed_policies = [
        "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess"
    ]

    try:
        iam.get_role(RoleName=ROLE_NAME)
        print(f"ℹ️ Role already exists: {ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            print("🛠️ Creating IAM role for AWS Glue...")
            iam.create_role(
                RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM Role for AWS Glue Crawlers and Jobs",
                MaxSessionDuration=3600
            )
            print(f"✅ Created role: {ROLE_NAME}")
        else:
            raise e

    # Attach required policies
    attached_policies = iam.list_attached_role_policies(RoleName=ROLE_NAME)["AttachedPolicies"]
    attached_arns = [p["PolicyArn"] for p in attached_policies]

    for policy_arn in managed_policies:
        if policy_arn not in attached_arns:
            iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn=policy_arn)
            print(f"🔗 Attached policy: {policy_arn}")
        else:
            print(f"ℹ️ Policy already attached: {policy_arn}")

    print("✅ IAM role verified and ready for Glue.\n")


# ==============================
# STEP 2: CREATE GLUE DATABASE
# ==============================
def create_glue_database():
    try:
        glue.create_database(DatabaseInput={"Name": DATABASE_NAME})
        print(f"✅ Glue Database created: {DATABASE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f"ℹ️ Database already exists: {DATABASE_NAME}")
        else:
            raise e


# ==============================
# STEP 3: CREATE AND START CRAWLERS
# ==============================
def create_and_run_crawlers():
    time.sleep(20)  # Ensure role propagation
    datasets = {
        "orders_crawler": f"s3://{RAW_BUCKET}/transactional/orders/",
        "order_items_crawler": f"s3://{RAW_BUCKET}/transactional/order_items/",
        "products_crawler": f"s3://{RAW_BUCKET}/product/"
    }

    for crawler_name, s3_path in datasets.items():
        try:
            glue.create_crawler(
                Name=crawler_name,
                Role=ROLE_NAME,
                DatabaseName=DATABASE_NAME,
                Description=f"Crawler for {crawler_name.replace('_', ' ')}",
                Targets={"S3Targets": [{"Path": s3_path}]},
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "DEPRECATE_IN_DATABASE"
                },
                TablePrefix=crawler_name.split("_")[0] + "_"
            )
            print(f"✅ Created crawler: {crawler_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                print(f"ℹ️ Crawler already exists: {crawler_name}")
            else:
                print(f"❌ Error creating crawler {crawler_name}: {e}")

    print("\n🚀 Starting all crawlers...")
    for crawler_name in datasets.keys():
        try:
            glue.start_crawler(Name=crawler_name)
            print(f"▶️ Started: {crawler_name}")
        except ClientError as e:
            print(f"⚠️ Could not start crawler {crawler_name}: {e}")


# ==============================
# STEP 4: WAIT FOR CRAWLERS TO COMPLETE
# ==============================
def wait_for_crawlers():
    print("\n⏳ Waiting for crawlers to complete...")
    crawlers = ["orders_crawler", "order_items_crawler", "products_crawler"]

    while True:
        all_done = True
        for name in crawlers:
            status = glue.get_crawler(Name=name)["Crawler"]["State"]
            print(f"🔍 {name}: {status}")
            if status != "READY":
                all_done = False
        if all_done:
            print("✅ All crawlers finished successfully!\n")
            break
        time.sleep(15)  # check every 15 seconds


# ==============================
# STEP 5: LIST TABLES FROM DATA CATALOG
# ==============================
def list_glue_tables():
    print("📊 Fetching tables from Glue Data Catalog...\n")
    try:
        tables = glue.get_tables(DatabaseName=DATABASE_NAME)["TableList"]
        for table in tables:
            print(f"✅ Table found: {table['Name']}")
    except ClientError as e:
        print(f"❌ Error listing tables: {e}")


# ==============================
# MAIN EXECUTION FLOW
# ==============================
if __name__ == "__main__":
    print("🚀 Setting up AWS Glue Infrastructure...\n")
    create_glue_role()
    create_glue_database()
    create_and_run_crawlers()
    wait_for_crawlers()
    list_glue_tables()
    print("\n🎉 Setup Complete: Glue Role, Crawlers & Tables Ready!")






"""This code works fine just merging it with glue_role.py"""
# import boto3
# from botocore.exceptions import ClientError

# # ========== CONFIGURATION ==========
# region = "us-east-1"
# glue_client = boto3.client("glue", region_name=region)

# database_name = "retail_lakehouse_db"
# iam_role = "AWSGlueServiceRole"  # 🔁 Replace with your actual Glue IAM role name
# raw_bucket = "apex-retail-raw-zone"

# # Define crawler sources
# datasets = {
#     "orders_crawler": f"s3://{raw_bucket}/transactional/orders/",
#     "order_items_crawler": f"s3://{raw_bucket}/transactional/order_items/",
#     "products_crawler": f"s3://{raw_bucket}/product/",
# }

# # ========== STEP 1: CREATE DATABASE ==========
# try:
#     glue_client.create_database(DatabaseInput={"Name": database_name})
#     print(f"✅ Glue Database created: {database_name}")
# except ClientError as e:
#     if e.response["Error"]["Code"] == "AlreadyExistsException":
#         print(f"ℹ️ Database already exists: {database_name}")
#     else:
#         print(f"❌ Error creating database: {e}")

# # ========== STEP 2: CREATE CRAWLERS ==========
# for crawler_name, s3_path in datasets.items():
#     try:
#         glue_client.create_crawler(
#             Name=crawler_name,
#             Role=iam_role,
#             DatabaseName=database_name,
#             Description=f"Crawler for {crawler_name.replace('_', ' ')}",
#             Targets={"S3Targets": [{"Path": s3_path}]},
#             # Schedule=None,  # Optional: set cron expression if you want auto-scheduling
#             SchemaChangePolicy={
#                 "UpdateBehavior": "UPDATE_IN_DATABASE",
#                 "DeleteBehavior": "DEPRECATE_IN_DATABASE",
#             },
#             TablePrefix=crawler_name.split("_")[0] + "_",
#         )
#         print(f"✅ Created crawler: {crawler_name}")
#     except ClientError as e:
#         if e.response["Error"]["Code"] == "AlreadyExistsException":
#             print(f"ℹ️ Crawler already exists: {crawler_name}")
#         else:
#             print(f"❌ Error creating crawler {crawler_name}: {e}")

# # ========== STEP 3: RUN ALL CRAWLERS ==========
# for crawler_name in datasets.keys():
#     try:
#         glue_client.start_crawler(Name=crawler_name)
#         print(f"🚀 Started crawler: {crawler_name}")
#     except ClientError as e:
#         print(f"❌ Could not start crawler {crawler_name}: {e}")

# print("\n🎉 All crawlers created and triggered successfully!")
