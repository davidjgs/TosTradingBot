import logging
import logging.config
import yaml

def getLogger(name, level=logging.INFO):
    #logging.basicConfig(level=level, filename='../../logs/trading_bot.log', format='%(asctime)s - t%(thread)d - %(name)s - %(levelname)s - %(message)s', )
    ##logging.config('config/')
    with open('../../config/log_config.yml', 'r') as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)

    logging.config.dictConfig(config)
    logger = logging.getLogger(name)
    return logger

def getLogger_rpa(name, level=logging.INFO):
    #logging.basicConfig(level=level, filename='../../logs/trading_bot.log', format='%(asctime)s - t%(thread)d - %(name)s - %(levelname)s - %(message)s', )
    ##logging.config('config/')
    with open('../../config/log_config_rpa.yml', 'r') as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)

    logging.config.dictConfig(config)
    logger = logging.getLogger(name)
    return logger