import re
import boto3
import json
import time
from datetime import datetime
import os
from pathlib import Path
import pandas as pd

from SharedData.Logger import Logger
from SharedData.SharedDataAWSKinesis import KinesisStreamConsumer

logger = Logger(__file__)
Logger.log.info('Starting Kinesis Stream Consumer Loop')
consumer = KinesisStreamConsumer()
stream = consumer.connect()
consumer.loop()