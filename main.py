import modules.S3TOLocal as S3TOLocal
import modules.cfg as cfg
from modules.YAMLReader import YAMLReader
from modules.Logger import logger

def execution(step, config):
    if(step == "S3TOLocal"):
        S3TOLocal.run(config)
    else:
        logger.error("Unknown step: %s", step)


def main():
    cfg.setup_parser()
    args = cfg.get_args()
    yaml = YAMLReader(args.config)
    yaml_dict = yaml.read_yaml()
    for step in yaml_dict.steps:
        logger.info("Request to run step: %s", step)
        if(yaml_dict[step] is None):
            logger.error("No configuration settings for  %s", step)
            execution(step)
        else:
            execution(step, yaml_dict[step])

if __name__ == '__main__':
    main()