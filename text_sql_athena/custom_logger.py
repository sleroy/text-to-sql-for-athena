
import logging
import sys

logger = logging.getLogger()

if not logger.handlers:
    logging.basicConfig(format='%(asctime)s,%(module)s,%(processName)s,%(levelname)s,%(message)s', level=logging.INFO, stream=sys.stderr)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
