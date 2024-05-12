from concurrent_log_handler import ConcurrentRotatingFileHandler
import logging

def setup_logging(log_name):
    logger = logging.getLogger(log_name)
    handler = ConcurrentRotatingFileHandler('application.log', "a", 512*1024, 5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
