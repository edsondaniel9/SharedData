import re
import boto3
import json
import time
from datetime import datetime
import os
from pathlib import Path
import pandas as pd

streamName = 'deepportfolio-logs'
profileName = 'kinesis-logs-write-only'

logfilepath = Path(os.environ['DATABASE_FOLDER']+'\\Logs\\')
logfilepath = logfilepath / (datetime.utcnow().strftime('%Y%m%d')+'.log')
dflogs = pd.DataFrame([])
if logfilepath.is_file():
    dflogs = pd.read_csv(logfilepath,header=None,sep=';')
    dflogs.columns = ['shardid','sequence_number','user_name','asctime','logger_name','level','message']

session = boto3.Session(profile_name=profileName)
client = session.client('kinesis')
stream = client.describe_stream(StreamName='deepportfolio-logs')
if stream and 'StreamDescription' in stream:
    stream = stream['StreamDescription']
    i=0    
    for i in range(len(stream['Shards'])):        
        shardid = stream['Shards'][i]['ShardId']
        if not dflogs.empty and (shardid in dflogs['shardid'].values):            
            seqnum = dflogs[dflogs['shardid']==shardid].iloc[-1]['sequence_number']
            shard_iterator = client.get_shard_iterator(
                StreamName=stream['StreamName'],
                ShardId=stream['Shards'][i]['ShardId'],
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=seqnum
                )
        else:
            shard_iterator = client.get_shard_iterator(
                StreamName=stream['StreamName'],
                ShardId=stream['Shards'][i]['ShardId'],                
                ShardIteratorType='TRIM_HORIZON'                
                )
        stream['Shards'][i]['ShardIterator'] = shard_iterator['ShardIterator']
if stream['StreamStatus'] != 'ACTIVE':
    raise Exception('Stream status %s' % (stream['StreamStatus']))

while True:
    try:
        for i in range(len(stream['Shards'])):
            response = client.get_records(\
                ShardIterator = stream['Shards'][i]['ShardIterator'],\
                Limit = 100)
            stream['Shards'][i]['ShardIterator'] = response['NextShardIterator']
            if len(response['Records'])> 0:
                for r in response['Records']:
                    rec = r['Data'].decode(encoding="UTF-8", errors="strict")
                    rec = json.loads(rec.replace("\'", "\""))
                    line = '%s;%s;%s;%s;%s' % (rec['user_name'],rec['asctime'],\
                        rec['logger_name'],rec['level'],rec['message']) 
                    print(line)
                    line = '%s;%s;%s;%s;%s;%s;%s' % (stream['Shards'][i]['ShardId'],\
                        r['SequenceNumber'],rec['user_name'],rec['asctime'],\
                        rec['logger_name'],rec['level'],rec['message'])                     
                    dt = datetime.strptime(rec['asctime'][:-5], '%Y-%m-%dT%H:%M:%S')
                    logfilepath = Path(os.environ['DATABASE_FOLDER']+'\\Logs\\')
                    logfilepath =logfilepath / (dt.strftime('%Y%m%d')+'.log')
                    with open(logfilepath,'a',encoding = 'utf-8') as f:
                        f.write(line+'\n')
        time.sleep(1)
    except Exception as e:
        print(e)
        break
    