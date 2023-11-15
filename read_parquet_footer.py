import boto3
from botocore.exceptions import NoCredentialsError

def read_last_chars_from_s3(bucket_name, object_key, num_chars):
    s3 = boto3.client('s3')

    try:
        # Get the object from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)

        # Read the last 'num_chars' characters
        content = response['Body'].read().decode('utf-8')
        last_chars = content[-num_chars:]

        return last_chars
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"An error occurred while reading from S3: {e}")
    return None

def search_strings_in_content(content):
    # Check if the strings "parquet-" and "cdh" exist
    if "parquet-" in content and "cdh" in content:
        print("Both strings 'parquet-' and 'cdh' exist in the last 1000 characters.")
    elif "parquet-" in content:
        print("String 'parquet-' exists in the last 1000 characters.")
    elif "cdh" in content:
        print("String 'cdh' exists in the last 1000 characters.")
    else:
        print("Neither 'parquet-' nor 'cdh' found in the last 1000 characters.")

# Replace these variables with your AWS S3 details
aws_access_key = 'your_access_key'
aws_secret_key = 'your_secret_key'
bucket_name = 'your_bucket_name'
object_key = 'your_object_key'
num_chars = 1000

# Read the last 1000 characters directly from S3
content = read_last_chars_from_s3(bucket_name, object_key, num_chars)

# Search for strings in the content
if content is not None:
    search_strings_in_content(content)
