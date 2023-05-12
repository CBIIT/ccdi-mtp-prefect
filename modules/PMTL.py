"""
RUN PMTL STEP TO ADD ANNOTATION TO A TARGET
\author Yizhen Chen

"""

import logging.config
import modules.Commons as commons
import json
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
import modules.Logger as Logger
logger = Logger.getLogger()


def readPMTLCSV(path):
    return  commons.readCSVToDict(path)

@task(name="CCDI-MTP : export PMTLed files")
def export(targetList,targetsFileName,path):
    # path correction
    if not path.endswith("/"):
        path = path + "/"
    commons.createDirectoryIfNotfound(path)

    #Save new target files
    with open(path+targetsFileName, mode="w") as outFile:
        for target in targetList:
            outFile.write(json.dumps(target, ensure_ascii=False) + "\n")
    logger.info("Save Processed Target file to %s", path+targetsFileName)

'''
Add PMTL propery to the target if match a record in the PMTL list

@:param 
    target    
    PMTLList
@:return target
'''
@task(name="CCDI-MTP : add PMTL propery to target")
def addPMTLProperyToTarget(target,PMTLList):
     target["pmtl_fda_designation"]=""
     for pmtl in PMTLList:
         #  target id matchs Ensembl_ID
         if pmtl[0]==target["id"]:
             logger.info("target -- %s --- matches PMTL list %s ", target["id"], pmtl[2])
             target["pmtl_fda_designation"]=pmtl[2]
     return target


@flow(name="CCDI-MTP : Setup Env, Read PMTL files")
def process(targetPath,PMTLList,config):
    logger = get_run_logger()
    targetsFileName = targetPath.split("/")[-1]
    targetsWithPMTL = []
    logger.info("Parsing target file  %st", targetsFileName)
    targets = commons.readJsonWithMultipleObj(targetPath)
    #Add PMTL propery into targets
    for target in targets:
        targetsWithPMTL.append(addPMTLProperyToTarget(target,PMTLList))

    export(targetsWithPMTL,targetsFileName,config['output'])


'''
Validate configuration settings, make sure 
pmtl, targets and outputs fields are presents

@:returns  boolean [True | False]
    True   is vaild configuration settings
    False  something wrong with the configuration settings
'''
def isValid(config):
    if "inputs" in config \
            and "output" in config:
        return  True
    else:
        return False

'''
Run PMTL process

@:param
config
    pmtl:
      - data/inputs/pmtl.csv
    targets:
      - data/inputs/targets/part-00000-2099be1d-059f-4e90-9263-7d205e2ba50f-c000.json
    outputs:
    - data/outputs/targets/part-00000-2099be1d-059f-4e90-9263-7d205e2ba50f-c000.json
    
@:return  
    generate new json file that contains targets along with PMTL property.   
'''
@flow(name="CCDI-MTP : Start PMTL annotation")
def run(config):
    logger = get_run_logger()
    logger.info(config)
    if isValid(config):
        logger.info("read PMTL list from %s", config["inputs"]["pmtl"])
        # reads pmtl list
        PMTLDic=readPMTLCSV(config["inputs"]["pmtl"])
        # read targets
        for targetPath in config["inputs"]["targets"]:
            # process each target file with PMTL list
            process(targetPath,PMTLDic,config)
    else:
        logger.error("InValid Configuration setting for PMTL")

