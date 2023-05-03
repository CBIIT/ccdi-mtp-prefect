import logging.config
import csv
import json
import os
import sys
import hashlib
import Logger as Logger
from prefect import task
logger = Logger.getLogger()
'''
check if dict has key or not,return key value if key exists, otherwise return default value
input : dict
        key  
        default value
output: key value or default value
'''
def check_dict_key(my_dict, key, default_value):
    if key not in my_dict:
        return default_value
    else:
        return my_dict[key]


'''
Delete file from local machine by given file path
input:  file path
output :N/A
'''
@task(name="Delete file from local machine by given file path")
def remove_file(file_path):
    try:
        os.remove(file_path)
        logger.info(f"{file_path} deleted successfully")
    except OSError as e:
        logger.error(f"Error deleting file: {e.filename} - {e.strerror}")


def validate_checksum(filename, type,expected_checksum):
    if type == "MD5":
        alg = hashlib.md5()
    elif type == "SHA-1":
        alg = hashlib.sha1()
    elif type == "SHA-2":
        alg = hashlib.sha256()
    else:
        return  False

    with open(filename, "rb") as f:
        # create a new MD5 hash object
        # read the file in chunks to avoid loading the whole file into memory at once
        chunk_size = 1024 * 1024  # 1 MB
        chunk = f.read(chunk_size)
        while chunk:
            alg.update(chunk)
            chunk = f.read(chunk_size)

    # compare the calculated checksum with the expected one
    if alg.hexdigest() == expected_checksum:
        return True
    return  False


def read_csv_to_dict(path):
    with open(path, mode='r') as csvfile:
        reader = csv.reader(csvfile)
        return list(reader)

def create_directory_if_not_found(path):
    if not os.path.exists(path):
        os.makedirs(path)

def read_json_with_multiple_Obj(path):
    jsonList = []
    with open(path) as f:
        for jsonObj in f:
            jsonList.append(json.loads(jsonObj))
    return jsonList


# (source): https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
def progress(count, total, status=''):
    bar_len = 100
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()