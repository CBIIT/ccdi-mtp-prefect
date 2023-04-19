"""
Download file from S3 to local
\author Yizhen Chen

"""
import boto3    
import os
from botocore.exceptions import ClientError
import modules.Commons as commons
import time
from prefect import task, flow, get_run_logger
import modules.Logger as Logger
logger = Logger.getLogger()

s3 = None
fails_tasks =[]
success_tasks =[]
skip_tasks=[]

'''
Validate overall tasks settings to have required fields and logic correctly, 
make sure if checksum validation is required, checksum type and value are provided as well. 

input :  task settings. a dict object derivative from yaml file
output: bool 
'''
@task(name="Validate individual task settings")
def isConfigValid(config):
    logger = get_run_logger()
    required_fields=[
        "s3-profile",
        "save-path",
        "s3-bucket",
        "overwrite",
        "type"
    ]
    logger.info("validate required fields in the configuration %s", required_fields)
    flag = True
    for field in required_fields:
        if field not in config:
            logger.error("Required field %s is not presented ", field)
            flag=False
    if "checksum-matching" in config and config["checksum-matching"] == True:
        if "checksum-type" not in config:
            logger.error("Checksum-matching is marked as true but required field checksum or checksum-type is not presented ")
            flag = False
    return flag



'''
download file from S3 to local.  
If overwrite is disabled, skip the download step when target download file is existing on local

input :  buck_name
          S3_key
          save_path
          overwrite
output: None (ERROR) / file_path (exists/downloaded)
'''

@task(name="download file from S3 to local")
def download_file(bucket_name: str, s3_key: str, save_path: str,overwrite: bool) -> str:
    logger = get_run_logger()
    save_path = save_path + s3_key
    # create directories if required directories does not exist
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))
    else:
        #if Download file exists and overwrite set False, then skip download step
        if not overwrite:
            logger.info("Download s3 file %s exists on local, ignore download step.", save_path)
            skip_tasks.append(bucket_name+"/"+s3_key)
            return save_path
    try:
        logger.info("Download s3 file %s  to %s.",s3_key, save_path)
        s3.Bucket(bucket_name).download_file(s3_key, save_path)
        logger.info("download s3 files %s success.", save_path)
        success_tasks.append(bucket_name+"/"+s3_key)
        return save_path
    except ClientError as e:
        logger.error("download files %s %s fails.",bucket_name,s3_key)
        logger.error("Error message  %s ",e)
        fails_tasks.append(bucket_name+"/"+s3_key)
        return None


@task(name="Setup AWS Client Session by assigning profile to use")
def setupAWSClient(profile_name):
    session = boto3.Session(profile_name=profile_name)
    global s3
    s3 = session.resource('s3')


'''
download folder from S3 to local.  
It iterates all all files with in a S3 folder and download them one by one

input :  buck_name
          folder_name
          save_path
          overwrite
output: N/A
'''
def download_folder(bucket_name: str, folder_name: str, save_path: str, overwrite: bool) -> str:
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=folder_name):
        # download sub folder
        if obj.key.endswith('/'):
            if obj.key!=folder_name:
                download_folder(bucket_name, obj.key, save_path,overwrite)
        else:
            download_file(bucket_name, obj.key, save_path,overwrite)

def process(s3_bucket,type,key,save_path,overwrite,checksum_matching,checksum_type,checksum):
    # validate if task has required attributes
        logger.info("Start download %s %s to %s",s3_bucket, key, save_path)
        if(type == "file"):
            file_path = download_file(s3_bucket,key,save_path,overwrite)
            if checksum_matching == True & commons.validate_checksum(file_path,checksum_type,checksum):
                logger.info("Pass Checksum validation - file %s  on local", save_path)
            else:
                logger.error("Fails Checksum validation - file %s  on local, deleting file", save_path)
                commons.remove_file(file_path)
        else:
            download_folder(s3_bucket,key,save_path,overwrite)



def do_download_jobs(config):
    if "s3-profile" in config:
        logger.info("setup s3-profile  %s", config["s3-profile"])
        setupAWSClient(config["s3-profile"])
    s3_bucket = config["s3-bucket"]
    type = commons.check_dict_key(config, "type", "file")
    save_path = config["save-path"]
    checksum_matching = commons.check_dict_key(config, "checksum-matching", False)
    checksum_type = commons.check_dict_key(config, "checksum-type", "")
    overwrite = commons.check_dict_key(config, "overwrite", True)

    #get list of download tasks
    list_of_download_tasks = config["files-to-download"]
    # process task one by one
    for task in list_of_download_tasks:
        # process each target file with RMTL list
        checksum = commons.check_dict_key(task, "checksum", "")
        process(s3_bucket,type,task["key"],save_path,overwrite,checksum_matching,checksum_type,checksum)



'''
main function 
'''
@flow(name="CCDI-MTP : Execute Job to Download files from S3 ")
def run(config):
    logger = get_run_logger()
    start_time = time.perf_counter()
    # get config
    logger.info(config)
    if isConfigValid(config):
        do_download_jobs(config)
    else:
        logger.error("InValid Configuration setting for S3 to Local")
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    execution_time_in_minutes = execution_time / 60.0
    logger.info("Job - Copy file from S3 to local finished. Execution time in minutes %s:", execution_time_in_minutes)
    logger.info("downloaded %s files", len(success_tasks))
    logger.info("skips %s files", len(skip_tasks))
    logger.info("fail to download %s files", len(fails_tasks))
    for task in fails_tasks:
        logger.info("fail to download %s ", task)