from email.policy import default
import os
import logging
import subprocess
import boto3
import awscli
from pathlib import Path
import pandas as pd
from datetime import datetime,timedelta
import time
import pytz
import json


STREAMNAME = 'deepportfolio-real-time'
PROFILENAME = 'kinesis-logs-write-only'

LOGSTREAMNAME = 'deepportfolio-logs'
LOGPROFILENAME = 'kinesis-logs-write-only'


# LOGS
class KinesisLogStreamHandler(logging.StreamHandler):
    #reference: https://docs.python.org/3/library/logging.html#logging.LogRecord
    def __init__(self):
        # By default, logging.StreamHandler uses sys.stderr if stream parameter is not specified
        logging.StreamHandler.__init__(self)
        
        self.__datastream = None
        self.__stream_buffer = []

        try:
            session = boto3.Session(profile_name=LOGPROFILENAME)
            self.__datastream = session.client('kinesis')
        except Exception:
            print('Kinesis client initialization failed.')

        self.__stream_name = LOGSTREAMNAME
        
    def emit(self, record):
        try:
            #msg = self.format(record)
            user = os.environ['USERNAME']+'@'+os.environ['USERDOMAIN']
            timezone = pytz.timezone("UTC")
            dt = datetime.utcfromtimestamp(record.created)
            dt= timezone.localize(dt)
            asctime = dt.strftime('%Y-%m-%dT%H:%M:%S%z')
            msg = {
                    'user_name': user,
                    'asctime': asctime,
                    'logger_name': record.name,
                    'level': record.levelname,
                    'message': record.msg,
                    'function_name': record.funcName,
                    'file_name': record.filename,                
                }   

            if self.__datastream:
                self.__stream_buffer.append({
                    'Data': str(msg).encode(encoding="UTF-8", errors="strict"),
                    'PartitionKey' : user,
                })
            else:
                stream = self.stream
                stream.write(msg)
                stream.write(self.terminator)

            self.flush()
        except Exception:
            self.handleError(record)

    def flush(self):
        self.acquire()

        try:
            if self.__datastream and self.__stream_buffer:
                self.__datastream.put_records(
                    StreamName=self.__stream_name,
                    Records=self.__stream_buffer                   
                )

                self.__stream_buffer.clear()
        except Exception as e:
            print("An error occurred during flush operation.")
            print(f"Exception: {e}")
            print(f"Stream buffer: {self.__stream_buffer}")
        finally:
            if self.stream and hasattr(self.stream, "flush"):
                self.stream.flush()

            self.release()

class KinesisLogStreamConsumer():
    def __init__(self,LOGSTREAMNAME=LOGSTREAMNAME, LOGPROFILENAME = LOGPROFILENAME):
        self.LOGSTREAMNAME=LOGSTREAMNAME
        self.LOGPROFILENAME=LOGPROFILENAME
        self.logfilepath = Path(os.environ['DATABASE_FOLDER']+'\\Logs\\')
        self.logfilepath = self.logfilepath / (datetime.utcnow().strftime('%Y%m%d')+'.log')
        self.lastlogfilepath = Path(os.environ['DATABASE_FOLDER']+'\\Logs\\')
        self.lastlogfilepath = self.lastlogfilepath / ((datetime.utcnow()+timedelta(days=-1)).strftime('%Y%m%d')+'.log')

    def readLogs(self):
        self.dflogs = pd.DataFrame([])
        if self.logfilepath.is_file():
            self.dflogs = pd.read_csv(self.logfilepath,header=None,sep=';')
            self.dflogs.columns = ['shardid','sequence_number','user_name','asctime','logger_name','level','message']
        
        if self.lastlogfilepath.is_file():
            _dflogs = pd.read_csv(self.lastlogfilepath,header=None,sep=';')
            _dflogs.columns = ['shardid','sequence_number','user_name','asctime','logger_name','level','message']
            self.dflogs = pd.concat([_dflogs,self.dflogs],axis=0)

        return self.dflogs
    
    def connect(self):                
        session = boto3.Session(profile_name=self.LOGPROFILENAME)
        self.client = session.client('kinesis')
        self.stream = self.client.describe_stream(StreamName=self.LOGSTREAMNAME)
        if self.stream and 'StreamDescription' in self.stream:
            self.stream = self.stream['StreamDescription']
            i=0    
            for i in range(len(self.stream['Shards'])):        
                shardid = self.stream['Shards'][i]['ShardId']
                if not self.dflogs.empty and (shardid in self.dflogs['shardid'].values):            
                    seqnum = self.dflogs[self.dflogs['shardid']==shardid].iloc[-1]['sequence_number']
                    shard_iterator = self.client.get_shard_iterator(
                        StreamName=self.stream['StreamName'],
                        ShardId=self.stream['Shards'][i]['ShardId'],
                        ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                        StartingSequenceNumber=seqnum
                        )
                else:
                    shard_iterator = self.client.get_shard_iterator(
                        StreamName=self.stream['StreamName'],
                        ShardId=self.stream['Shards'][i]['ShardId'],                
                        ShardIteratorType='TRIM_HORIZON'                
                        )
                self.stream['Shards'][i]['ShardIterator'] = shard_iterator['ShardIterator']
        if self.stream['StreamStatus'] != 'ACTIVE':
            raise Exception('Stream status %s' % (self.stream['StreamStatus']))
        
        return self.stream

    def loop(self):                
        while True:        
            for i in range(len(self.stream['Shards'])):
                response = self.client.get_records(\
                    ShardIterator = self.stream['Shards'][i]['ShardIterator'],\
                    Limit = 100)
                self.stream['Shards'][i]['ShardIterator'] = response['NextShardIterator']
                if len(response['Records'])> 0:
                    for r in response['Records']:
                        try:
                            rec = r['Data'].decode(encoding="UTF-8", errors="strict")                        
                            rec = json.loads(rec.replace("\'", "\""))
                            line = '%s;%s;%s;%s;%s' % (rec['user_name'],rec['asctime'],\
                                rec['logger_name'],rec['level'],rec['message']) 
                            print(line)
                            line = '%s;%s;%s;%s;%s;%s;%s' % (self.stream['Shards'][i]['ShardId'],\
                                r['SequenceNumber'],rec['user_name'],rec['asctime'],\
                                rec['logger_name'],rec['level'],rec['message'])                     
                            dt = datetime.strptime(rec['asctime'][:-5], '%Y-%m-%dT%H:%M:%S')
                            logfilepath = Path(os.environ['DATABASE_FOLDER']+'\\Logs\\')
                            logfilepath =logfilepath / (dt.strftime('%Y%m%d')+'.log')
                            if not logfilepath.parents[0].is_dir():
                                os.makedirs(logfilepath.parents[0])
                            with open(logfilepath,'a+',encoding = 'utf-8') as f:
                                f.write(line+'\n')  
                        except Exception as e:
                            print('Invalid record:'+str(e))
            time.sleep(1)
        
# REAL TIME
class KinesisStreamProducer():
    def __init__(self,STREAMNAME=STREAMNAME, PROFILENAME = PROFILENAME):
        self.__datastream = None
        self.__stream_buffer = []
        try:
            session = boto3.Session(profile_name=PROFILENAME)
            self.__datastream = session.client('kinesis')
        except Exception:
            print('Kinesis client initialization failed.')
        self.__stream_name = STREAMNAME

    def produce(self, record, partitionkey):                
        self.__stream_buffer.append({
            'Data': str(record).encode(encoding="UTF-8", errors="strict"),
            'PartitionKey' : partitionkey,
        })
        self.__datastream.put_records(
                    StreamName=self.__stream_name,
                    Records=self.__stream_buffer                   
                )
        self.__stream_buffer = []

class KinesisStreamConsumer():
    def __init__(self,STREAMNAME=STREAMNAME, PROFILENAME = PROFILENAME):
        self.STREAMNAME=STREAMNAME
        self.PROFILENAME=PROFILENAME        

    def connect(self):                
        session = boto3.Session(profile_name=self.PROFILENAME)
        self.client = session.client('kinesis')
        self.stream = self.client.describe_stream(StreamName=self.STREAMNAME)
        if self.stream and 'StreamDescription' in self.stream:
            self.stream = self.stream['StreamDescription']
            i=0    
            for i in range(len(self.stream['Shards'])):        
                shardid = self.stream['Shards'][i]['ShardId']
                shard_iterator = self.client.get_shard_iterator(
                    StreamName=self.stream['StreamName'],
                    ShardId=self.stream['Shards'][i]['ShardId'],                
                    ShardIteratorType='LATEST'                
                    )
                self.stream['Shards'][i]['ShardIterator'] = shard_iterator['ShardIterator']
        if self.stream['StreamStatus'] != 'ACTIVE':
            raise Exception('Stream status %s' % (self.stream['StreamStatus']))
        
        return self.stream

    def loop(self):                
        while True:        
            for i in range(len(self.stream['Shards'])):
                response = self.client.get_records(\
                    ShardIterator = self.stream['Shards'][i]['ShardIterator'],\
                    Limit = 100)
                self.stream['Shards'][i]['ShardIterator'] = response['NextShardIterator']
                if len(response['Records'])> 0:
                    for r in response['Records']:
                        try:
                            rec = r['Data'].decode(encoding="UTF-8", errors="strict")                        
                            #rec = json.loads(rec.replace("\'", "\""))                            
                            print(rec)
                        except Exception as e:
                            print('Invalid record:'+str(e))
            time.sleep(1)
     