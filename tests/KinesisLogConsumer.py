from SharedData.Logger import Logger
from SharedData.SharedDataAWSKinesis import KinesisLogStreamConsumer

logger = Logger(__file__)
Logger.log.info('Starting Kinesis Logs Stream Consumer Loop')
consumer = KinesisLogStreamConsumer()
dflogs = consumer.readLogs()
stream = consumer.connect()
consumer.loop()