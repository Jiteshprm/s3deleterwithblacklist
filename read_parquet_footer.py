import boto3
import io

def read_last_chars_from_s3_binary_file(bucket_name, key, num_chars=1000):
    s3 = boto3.client('s3')

    # Get the object's metadata to determine its size
    response = s3.head_object(Bucket=bucket_name, Key=key)
    content_length = response['ContentLength']

    # Calculate the range for the last 'num_chars' bytes
    start_byte = max(0, content_length - num_chars)
    range_header = f"bytes={start_byte}-{content_length-1}"

    # Request the specified range of bytes from S3
    response = s3.get_object(Bucket=bucket_name, Key=key, Range=range_header)
    content = response['Body'].read()

    return content

def search_and_print_patterns(data):
    parquet_pattern = b'parquet-'
    cdh_pattern = b'cdh'

    parquet_start = data.find(parquet_pattern)

    if parquet_start != -1:
        # Find the next occurrence of the null character (0) after parquet-
        null_char_index = data.find(b'\x00', parquet_start)
        if null_char_index != -1:
            parquet_end = null_char_index
            parquet_data = data[parquet_start:parquet_end].decode('utf-8')
            print(f"Found parquet- pattern: {parquet_data}")
        else:
            print("parquet- pattern found, but no null character (0) after it.")
    
    cdh_start = data.find(cdh_pattern)
    if cdh_start != -1:
        print("Found cdh pattern.")

# Replace 'your_bucket_name' and 'your_file_key' with the actual S3 bucket name and file key
bucket_name = 'your_bucket_name'
file_key = 'your_file_key'

# Read the last 1000 characters from the binary file in S3
binary_data = read_last_chars_from_s3_binary_file(bucket_name, file_key)

# Search for patterns and print results
search_and_print_patterns(binary_data)
