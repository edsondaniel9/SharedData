import os
import logging
import boto3
from pathlib import Path
from datetime import datetime, timedelta
import glob
import pandas as pd
from pythonjsonlogger.jsonlogger import JsonFormatter
import boto3

from dotenv import load_dotenv

from SharedData.SharedDataAWSKinesis import KinesisStreamHandler

class Logger:

    log = None
    DATABASE_FOLDER = 'C:/DB'

    def __init__(self, source):
        self.source = source

        load_dotenv()  # take environment variables from .env.
        
        if 'SOURCE_FOLDER' in os.environ:
            commompath = os.path.commonpath([source,os.environ['SOURCE_FOLDER']])
            source = source.replace(commompath,'')
        source = source.lstrip('\\').lstrip('/')
        source = source.replace('.py','')

        if not 'DATABASE_FOLDER' in os.environ:
            os.environ['DATABASE_FOLDER'] = Logger.DATABASE_FOLDER

        path = Path(os.environ['DATABASE_FOLDER'])
        path = path / 'Logs'
        path = path / datetime.now().strftime('%Y%m%d')
        path = path / (os.environ['USERNAME']+'@'+os.environ['USERDOMAIN'])        
        path = path / (source+'.log')
        if not path.parents[0].is_dir():
            os.makedirs(path.parents[0])
        os.environ['LOG_PATH'] = str(path)

        if 'LOG_LEVEL' in os.environ:
            if os.environ['LOG_LEVEL']=='DEBUG':
                loglevel = logging.DEBUG
            elif os.environ['LOG_LEVEL']=='INFO':
                loglevel = logging.INFO   
        else:
            loglevel = logging.INFO

        # Create Logger
        self.log = logging.getLogger(source)
        self.log.setLevel(logging.DEBUG)    
        formatter = logging.Formatter(os.environ['USERNAME']+'@'+os.environ['USERDOMAIN'] + 
            ';%(asctime)s;%(name)s;%(levelname)s;%(message)s',\
            datefmt='%Y-%m-%dT%H:%M:%S%z')
        #log screen
        handler = logging.StreamHandler()
        handler.setLevel(loglevel)    
        handler.setFormatter(formatter)
        self.log.addHandler(handler)
        #log file        
        fhandler = logging.FileHandler(os.environ['LOG_PATH'], mode='a')
        fhandler.setLevel(loglevel)    
        fhandler.setFormatter(formatter)
        self.log.addHandler(fhandler)
        #log to aws kinesis
        kinesishandler = KinesisStreamHandler()
        kinesishandler.setLevel(logging.DEBUG)
        jsonformatter = JsonFormatter(os.environ['USERNAME']+'@'+os.environ['USERDOMAIN'] + 
            ';%(asctime)s;%(name)s;%(levelname)s;%(message)s',\
            datefmt='%Y-%m-%dT%H:%M:%S%z')
        kinesishandler.setFormatter(jsonformatter)
        self.log.addHandler(kinesishandler)
        #assign static variable log to be used by modules
        Logger.log = self.log
        Logger.log.debug('Logger initialized!')
        
    def readLogs(self):    
        logsdir = Path(os.environ['LOG_PATH']).parents[0]
        lenlogsdir = len(str(logsdir))
        files = glob.glob(str(logsdir) + "/**/*.log", recursive = True)   
        df = pd.DataFrame()
        # f=files[0]
        for f in files:
            source = f[lenlogsdir+1:].replace('.log','.py')
            try:
                _df = pd.read_csv(f,header=None,sep=';')
                _df.columns = ['user','datetime','name','type','message']
                _df['source'] = source
                df = df.append(_df)
            except Exception as e:
                print(e)
                pass
        return df
