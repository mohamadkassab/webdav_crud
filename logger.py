from datetime import datetime
import logging
from static import Credentials

class Logger :
    def myLogger(self):
       now = datetime.now()
       logger=logging.getLogger('ProvisioningPython')
       #  file = (Credentials.local_path_logfolder + now.strftime("%Y-%m-%d") +'.log')
       if not len(logger.handlers):
          logger.setLevel(logging.DEBUG)
          handler=logging.FileHandler(Credentials.local_path_logfolder + now.strftime("%Y-%m-%d") +'.log')
          formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
          handler.setFormatter(formatter)
          logger.addHandler(handler)
          
       return logger