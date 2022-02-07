import re
import boto3
import json
import time
from datetime import datetime
import os
from pathlib import Path
import pandas as pd

from SharedData.Logger import Logger,KinesisStreamConsumer

streamName = 'deepportfolio-logs'
profileName = 'kinesis-logs-write-only'


consumer = KinesisStreamConsumer()
dflogs = consumer.readLogs()
stream = consumer.connect()
consumer.loop()