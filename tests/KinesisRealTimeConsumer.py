from dotenv import load_dotenv
load_dotenv()
import os,sys
sys.path.insert(0,os.environ['SOURCE_FOLDER'])

from SharedData.Logger import Logger
from SharedData.SharedDataAWSKinesis import KinesisStreamConsumer

logger = Logger(__file__)
Logger.log.info('Starting Kinesis Stream Consumer Loop')
consumer = KinesisStreamConsumer()
stream = consumer.connect()
consumer.loop()