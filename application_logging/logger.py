import logging
from datetime import datetime
import os
LOG_DIR = "logs"
TODAY = datetime.now()
LOGNAME = str(TODAY.strftime("%m")) + "_" + str(TODAY.year)

def get_log_file_name():
    return f"{LOGNAME}.log"


LOG_FILE_NAME = get_log_file_name()

os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

logging.basicConfig(filename=LOG_FILE_PATH,
                    filemode="a",
                    format='[%(asctime)s] \t%(levelname)s \t%(lineno)d \t%(filename)s \t%(funcName)s() \t%(message)s',
                    level=logging.INFO
                    )

logger = logging.getLogger("Scrape")
