import os,sys
import logging
import boto3
from pathlib import Path
from datetime import datetime, timedelta
import glob
import pandas as pd
from pythonjsonlogger.jsonlogger import JsonFormatter
import boto3
from dotenv import load_dotenv

from SharedData.SharedDataAWSKinesis import KinesisLogStreamHandler


load_dotenv()  # take environment variables from .env.

if not 'DATABASE_FOLDER' in os.environ:    
    os.environ['DATABASE_FOLDER'] = os.path.expanduser("~")+'\DB' 

if not 'S3_BUCKET' in os.environ:    
    os.environ['S3_BUCKET'] = 's3://deepportfolio'

if not 'LOG_STREAMNAME' in os.environ:    
    os.environ['LOG_STREAMNAME'] = 'deepportfolio-logs'

if not 'LOG_PROFILENAME' in os.environ:    
    os.environ['LOG_PROFILENAME'] = 'kinesis-logs-write-only'


class Logger:

    log = None

    def __init__(self, source):
        os.environ['PATH'] = os.environ['PATH']+';'+';'.join(sys.path)
                
        if 'SOURCE_FOLDER' in os.environ:
            commompath = os.path.commonpath([source,os.environ['SOURCE_FOLDER']])
            source = source.replace(commompath,'')

        source = source.lstrip('\\').lstrip('/')
        source = source.replace('.py','')
        self.source = source

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
        kinesishandler = KinesisLogStreamHandler()
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
                _df = pd.read_csv(f,header=None,sep=';',\
                    error_bad_lines=False,engine='python')
                _df.columns = ['user','datetime','name','type','message']
                _df['source'] = source
                df = df.append(_df)
            except Exception as e:
                print('Read logs error:'+str(e))

        return df
