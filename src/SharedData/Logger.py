import os, sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

def getPath(config):
    path = Path(config.db_directory)
    path = path / 'Logs'
    path = path / datetime.now().strftime('%Y%m%d')
    path = path / (config.username+'@'+config.userdomain)
    if not path.is_dir():
        os.makedirs(path)
    return path

def setLogger(filepath,config):
    n = len(config.source_directory)
    filepath = filepath[n+1:]
    filepath = filepath.replace('.py','')

    logger = logging.getLogger(filepath)
    logger.setLevel(config.verbose_level)
    formatter = logging.Formatter('%(levelname)s;%(asctime)s;%(message)s',\
         datefmt='%H:%M:%S')    
    #log screen
    handler = logging.StreamHandler()
    handler.setLevel(config.verbose_level)    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    #log file    
    config.logs_directory = getPath(config)
    logpath = config.logs_directory / (filepath+'.log')
    if not logpath.parents[0].is_dir():
        os.makedirs(logpath.parents[0]) 
    fhandler = logging.FileHandler(logpath, mode='a')
    fhandler.setLevel(config.logging_level)    
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    
    config.logger = logger    
