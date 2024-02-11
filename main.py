import json,subprocess


from botocore.exceptions import NoCredentialsError
import tarfile, boto3, os


def file_compression(target, sources):
    with tarfile.open(target, 'w:gz') as tar:
        for file in sources:
            tar.add(file, arcname=os.path.basename(file))

    print(f"Compression complete. Archive saved as: {target}")

def decompress_tarfile(input_tar_path, output_folder):
    try:
        with tarfile.open(input_tar_path, 'r') as tar:
            # Extract all contents of the tar file to the output directory
            tar.extractall(output_folder)

        print(f'Successfully decompressed {input_tar_path} to {output_folder}')
    except Exception as e:
        print(f'Error: {e}')

def download_s3_bucket(bucket_name, local_directory):
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                # Create local file path based on the object key
                local_file_path = os.path.join(local_directory, obj['Key'])

                # Ensure the local directory exists
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                # Download the file from S3 to the local directory
                s3.download_file(bucket_name, obj['Key'], local_file_path)
                print(f"File '{obj['Key']}' downloaded to '{local_file_path}'.")
        else:
            print(f"No objects found in '{bucket_name}'.")
    except NoCredentialsError:
        print("AWS credentials not available.")


def list_files(dir):
    try:
        # List all files in the specified directory
        files = ["/tmp/"+f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]

        if files:
            print(files)
            return files
        else:
            print(f"No files found in '{dir}'.")
    except FileNotFoundError:
        print(f"Directory '{dir}' not found.")


def upload_file_to_s3(file_path, bucket_name, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)

    s3 = boto3.client('s3')

    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"File '{file_path}' uploaded to '{bucket_name}' as '{object_name}'.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except NoCredentialsError:
        print("AWS credentials not available.")


def list_objects_in_bucket(bucket_name):
    s3 = boto3.client('s3')

    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print(f"Objects in '{bucket_name}':")
            for obj in response['Contents']:
                print(f"- {obj['Key']}")
        else:
            print(f"No objects found in '{bucket_name}'.")
    except NoCredentialsError:
        print("AWS credentials not available.")



def lambda_handler(event, context):
    # TODO implement
    # # Example usage:
    bucket_name = 'testbucket007tbd'
    local_directory = '/tmp'
    target = '/tmp/compressed_data_1_3.tar.gz'
    download_s3_bucket(bucket_name, local_directory)
    files = list_files(local_directory)
    file_compression(target, files)
    command = 'chmod 755 /tmp/compressed_data.tar.gz'
    subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
    upload_file_to_s3(target, bucket_name)
    list_objects_in_bucket(bucket_name)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


