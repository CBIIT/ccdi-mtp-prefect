from prefect import flow, get_run_logger
import requests
import yaml
from addict import Dict
import modules.S3ToLocal as S3ToLocal
import modules.Logger as Logger
logger = Logger.getLogger()

def execution(step, config):
    if(step == "S3ToLocal"):
        S3ToLocal.run(config)
    else:
        logger.error("Unknown step: %s", step)


@flow(name="mtp_etl_pipeline")
def main(url):
    logger = get_run_logger()
    try:
        response = requests.get(url)
        yaml_data = yaml.safe_load(response.content)
        yaml_dictionary = Dict(yaml_data)
    except (IOError,yaml.YAMLError)as exc:
        logger.error("There is an issue to load configuration settings %s", url)
    yaml_dict = yaml_dictionary

    for step in yaml_dict.steps:
        logger.info("Request to run step: %s", step)
        if(yaml_dict[step] is None):
            logger.error("No configuration settings for  %s", step)
            execution(step)
        else:
            execution(step, yaml_dict[step])



if __name__ == '__main__':
    url = "https://raw.githubusercontent.com/CBIIT/ccdi-mtp-prefect/dev/config.yaml"
    main(url)