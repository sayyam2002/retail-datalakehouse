import shutil
import sys
import os
import subprocess
from dotenv import load_dotenv

def configure_aws_cli_from_env():
    load_dotenv()

    access_key = os.getenv("AWS_IAM_USER_ACCESS_KEY")
    secret_key = os.getenv("AWS_IAM_USER_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_IAM_USER_REGION_ID", "us-east-1")
    output_format = os.getenv("AWS_IAM_USER_FORMAT", "json")

    if not access_key or not secret_key:
        print("❌ Error: AWS Access Key or Secret Key not found in .env file.")
        return

    # ✅ Find aws.exe path
    aws_path = shutil.which("aws")
    if not aws_path:
        print("⚠️ AWS CLI executable not found in PATH. Trying to locate in Scripts folder...")
        # Try common install locations
        possible_paths = [
            os.path.join(sys.prefix, "Scripts", "aws.exe"),
            os.path.join(sys.prefix, "bin", "aws"),
        ]
        for p in possible_paths:
            if os.path.exists(p):
                aws_path = p
                break

    if not aws_path:
        print("❌ AWS CLI not found. Run this command first:")
        print("   python -m pip install awscli")
        print("   and ensure Scripts folder is in your PATH.")
        return

    print(f"🔍 Using AWS CLI at: {aws_path}")

    try:
        print("=== Configuring AWS CLI using .env values ===")
        subprocess.run([aws_path, "configure", "set", "aws_access_key_id", access_key], check=True)
        subprocess.run([aws_path, "configure", "set", "aws_secret_access_key", secret_key], check=True)
        subprocess.run([aws_path, "configure", "set", "region", region], check=True)
        subprocess.run([aws_path, "configure", "set", "output", output_format], check=True)

        print("\n✅ AWS CLI configured successfully!")

        print("\n🔍 Verifying AWS credentials...")
        result = subprocess.run([aws_path, "sts", "get-caller-identity"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Credentials verified successfully!\n")
            print(result.stdout)
        else:
            print("⚠️ Failed to verify credentials. Check your keys or permissions.")
            print(result.stderr)

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Command failed: {e}")

if __name__ == "__main__":
    configure_aws_cli_from_env()