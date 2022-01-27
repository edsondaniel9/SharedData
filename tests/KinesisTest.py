import boto3
import json
import time
import os

session = boto3.Session(profile_name='kinesis-logs-write-only')
client = session.client('kinesis')

stream_details = client.describe_stream(StreamName='deepportfolio-logs')

shard_ids = []
stream_name = None 
if stream_details and 'StreamDescription' in stream_details:
    stream_name = stream_details['StreamDescription']['StreamName']
    for shard_id in stream_details['StreamDescription']['Shards']:
         shard_id = shard_id['ShardId']
         shard_iterator = client.get_shard_iterator(
             StreamName=stream_name,
             ShardId=shard_id,
             ShardIteratorType='TRIM_HORIZON'
             )
         shard_ids.append({'shard_id' : shard_id ,'shard_iterator' : shard_iterator['ShardIterator'] })

while True:
    for sid in shard_ids:
        shard_id = sid['shard_id']
        response = client.get_records(ShardIterator = sid['shard_iterator'], Limit = 10)
        sid['shard_iterator'] = response['NextShardIterator']
        if len(response['Records'])> 0:
            print(response['Records'])
    time.sleep(1)
    