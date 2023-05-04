"""
Download file from FTP to local
\author Yizhen Chen

"""
from ftplib import FTP
import os
import time
import Commons as commons
from prefect import task, flow, get_run_logger
import Logger as Logger
logger = Logger.getLogger()


HOSTNAME = "test"
USERNAME = "username@email.com"
PASSWORD = "pwd"
isAnonymous = True
OUTPUT_FOLDER = ""
skip_tasks=[]
success_tasks=[]
fails_tasks=[]

@task(name="connect to FTP")
def connect(HOSTNAME,isAnonymous = True ,USERNAME = "username@email.com" ,PASSWORD = "pwd"):
    logger = get_run_logger()
    try:
        if isAnonymous:
            logger.info("Connect to FTP: %s", HOSTNAME)
            ftp_server = FTP(HOSTNAME)
        else:
            logger.info("Connect to FTP: %s , %s ", HOSTNAME,USERNAME)
            ftp_server = FTP(HOSTNAME, USERNAME, PASSWORD)

        # force UTF-8 encoding
        ftp_server.encoding = "utf-8"
        return login(ftp_server)
    except:
        logger.error("Connect to FTP: %s", HOSTNAME)
        logger.error("Something  wrong with connect to FTP")

def login(ftp_server):

    logger.info("login to FTP")
    ftp_server.login()
    logger.info(ftp_server.getwelcome())
    return  ftp_server;


def close(ftp_server):
    logger.info("close connection")
    ftp_server.close();

# create directories recursively
# path: /home/dail/first/second/third
def makeParentDirs(path):
    dirname = os.path.dirname(path)
    while not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
            logger.info("created {0}".format(dirname))
        except OSError as e:
            logger.error("Get ERROR when creating directories recursively : %s", e)

@task(name="download files")
def downloadFile(ftp_server, filename, dest,overwrite= False, checksum_matching= False,checksum_type="md5",checksum=""):
        logger = get_run_logger()
        logger.info("download file %s", filename)
        dirs = dest + filename
        if not os.path.exists(os.path.dirname(dirs)):
            makeParentDirs(dirs)
        if os.path.exists(os.path.dirname(dirs)):
            # if Download file exists and overwrite set False, then skip download step
            if not overwrite:
                logger.info("Download FTP file %s exists on local, ignore download step.", filename)
                skip_tasks.append(filename)
                return

        logger.info("Downloading file :  {0}".format(filename))
        logger.info("Saving  file to  file :  {0}".format(os.path.dirname(dirs)))
        try:
            with open(dirs, "wb") as file:
                # Command for Downloading the file "RETR filename"
                ftp_server.retrbinary(f"RETR {filename}", file.write)
                success_tasks.append(filename)
        except IOError:
                logger.error("An error occurred while reading the file. %s", dirs)
                fails_tasks.append(filename)

        if checksum_matching == True:
            if commons.validate_checksum(dirs, checksum_type, checksum):
                    logger.info("Pass Checksum validation - file %s  on local", dirs)
            else:
                logger.error("Fails Checksum validation - file %s  on local, deleting file", dirs)
                commons.remove_file(dirs)
        else:
            logger.info("Skip Checksum validation - file %s", dirs)



def isDirectory(item):
        if "." not in item and not item.endswith("_SUCCESS"):
            # this is a folder
            return True
        return False


# find files in a directory with specific file_extension
@task(name="find files in a directory with specific file_extension")
def findFilesInDir(ftp,path, file_extension="n/a"):
    logger = get_run_logger()
    logger.info("find files in a %s with specific file_extension  %s", path , file_extension)
    logger.info("find files in  %s with specific file_extension  %s", path , file_extension)
    files = []
    # direct to the ftp path
    ftp.cwd(path)

    # list files in the folder
    folders = ftp.nlst()

    while len(folders) > 0:

        item = folders.pop()
        item_path = path + item
        if isDirectory(item):
            #add items in that directory to folders
            ls = ftp.nlst(item)
            folders.extend(ls)
        else:
            if file_extension!= "n/a":
                if item.endswith(file_extension):
                    files.append(item_path)
            else:
                files.append(item_path)

    ftp.cwd("/")
    return files

@flow
def do_download_jobs(config):

    HOSTNAME = config["host-name"]
    isAnonymous = config["is_anonymous"]
    userName=commons.check_dict_key(config, "username", "username")
    password=commons.check_dict_key(config, "password", "password")
    checksum_matching = commons.check_dict_key(config, "checksum-matching", False)
    checksum_type = commons.check_dict_key(config, "checksum-type", "")
    overwrite = commons.check_dict_key(config, "overwrite", False)
    OUTPUT_FOLDER = commons.check_dict_key(config, "save-path", "/Users/cheny39/Documents/work/tmp/tmp/")
    isFolder = commons.check_dict_key(config, "type", "file")

    file_extension =commons.check_dict_key(config, "file_extension", "n/a")
    #Connect to FTP
    ftp = connect(HOSTNAME)
    #get list of download tasks
    list_of_download_tasks = config["files-to-download"]


    if  isFolder != "file" :
        # process task one by one
        for task in list_of_download_tasks:
            FTP_PATH = task["path"]

            # Get all Json Files listed in FTP_PATH
            files = findFilesInDir(ftp,FTP_PATH, file_extension)

            checksum_matching = False
            checksum=""
            #Download Json Files one by one
            for file in files:
                downloadFile(ftp,file,OUTPUT_FOLDER, overwrite,checksum_matching,checksum_type, checksum)
    else:
        for task in list_of_download_tasks:
            file = task["path"]
            checksum=commons.check_dict_key(task, "checksum", "")
            # Download Json Files one by one
            downloadFile(ftp, file, OUTPUT_FOLDER, overwrite, checksum_matching, checksum_type, checksum)

    close(ftp)


'''
main function 
'''
@flow(name="CCDI-MTP : Execute Job to Download files from FTP ")
def run(config):
    logger = get_run_logger()
    start_time = time.perf_counter()
    # get config
    logger.info(config)
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    execution_time_in_minutes = execution_time / 60.0
    do_download_jobs(config)
    logger.info("Job - Download file from FTP to local finished. Execution time in minutes %s:", execution_time_in_minutes)
    logger.info("downloaded %s files", len(success_tasks))
    logger.info("skips %s files", len(skip_tasks))
    logger.info("fail to download %s files", len(fails_tasks))


