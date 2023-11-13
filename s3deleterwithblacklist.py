import boto3
import csv
import datetime

def delete_files_except_blacklist(s3_client, bucket, prefix, blacklist_file):
    blacklist = load_blacklist(blacklist_file)

    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}

    for page in paginator.paginate(**operation_parameters):
        if 'Contents' in page:
            for content in page['Contents']:
                key = content['Key']

                # Check if the key is in the blacklist or the date is not within the specified range
                if is_in_blacklist(key, blacklist):
                    print(f"Skipping: s3://{bucket}/{key} (in blacklist)")
                else:
                    print(f"Deleting: s3://{bucket}/{key}")
                    s3_client.delete_object(Bucket=bucket, Key=key)

def load_blacklist(blacklist_file):
    blacklist = []

    with open(blacklist_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            s3_path = row['s3_path']
            start_date = datetime.datetime.strptime(row['start_date'], '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(row['end_date'], '%Y-%m-%d').date()
            blacklist.append({'s3_path': s3_path, 'start_date': start_date, 'end_date': end_date})

    return blacklist

def is_in_blacklist(key, blacklist):
    for entry in blacklist:
        blacklist_path = entry['s3_path']
        start_date = entry['start_date']
        end_date = entry['end_date']

        if key.startswith(blacklist_path) and start_date <= datetime.datetime.strptime(key.split('/')[1], '%Y-%m-%d').date() <= end_date:
            return True

    return False

if __name__ == "__main__":
    # Set your AWS credentials and region (make sure they are configured properly)
    aws_access_key_id = 'YOUR_ACCESS_KEY_ID'
    aws_secret_access_key = 'YOUR_SECRET_ACCESS_KEY'
    aws_region = 'YOUR_REGION'

    # Set the S3 bucket, prefix (path), and blacklist CSV file
    s3_bucket = 'your-s3-bucket'
    s3_prefix = 'your-s3-prefix'
    blacklist_file = 'path_to_your_blacklist.csv'

    # Create an S3 client
    s3_client = boto3.client('s3', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Delete files except those in the blacklist
    delete_files_except_blacklist(s3_client, s3_bucket, s3_prefix, blacklist_file)
