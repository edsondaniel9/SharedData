import re
import boto3
import json
import time
from datetime import datetime
import os
from pathlib import Path
import pandas as pd

from SharedData.Logger import Logger
from SharedData.SharedDataAWSKinesis import KinesisLogStreamConsumer

logger = Logger(__file__)
Logger.log.info('Starting Kinesis Logs Stream Consumer Loop')
consumer = KinesisLogStreamConsumer()
dflogs = consumer.readLogs()
stream = consumer.connect()
consumer.loop()