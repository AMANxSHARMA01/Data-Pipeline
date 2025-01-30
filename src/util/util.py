import boto3
import os
import shutil

def create_folder_recursively_if_not_exist(folder_path: str):
    """Creates a folder and its parent directories if they do not exist."""
    os.makedirs(folder_path, exist_ok=True)

def delete_folder_recursively(folder_path: str):
    """Deletes a folder and all its contents recursively."""
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        shutil.rmtree(folder_path)

def delete_file(file_path: str):
    """Deletes a file if it exists."""
    if os.path.exists(file_path) and os.path.isfile(file_path):
        os.remove(file_path)


s3_client = boto3.client("s3")

def create_folder_recursively_if_not_exist_aws(bucket_name: str, folder_path: str):
    """Creates a 'folder' (empty object with '/') in S3 if it does not exist."""
    if not folder_path.endswith("/"):
        folder_path += "/"
    s3_client.put_object(Bucket=bucket_name, Key=folder_path)

def delete_folder_recursively_aws(bucket_name: str, folder_path: str):
    """Deletes a folder and all its contents recursively in S3."""
    if not folder_path.endswith("/"):
        folder_path += "/"
    
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    if "Contents" in objects_to_delete:
        delete_keys = [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]
        s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})

def delete_file_aws(bucket_name: str, file_path: str):
    """Deletes a file in S3."""
    s3_client.delete_object(Bucket=bucket_name, Key=file_path)
