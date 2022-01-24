from SharedData.Logger import Logger
logger = Logger(__file__)

import numpy as np
message = 'Logging test %2.10f' % (np.random.rand())
Logger.log.info(message)
dflogs = logger.readLogs()
if (dflogs.iloc[-1]['message'] == message):
    Logger.log.info('logging works!')
else:
    raise Exception('SharedData logging error')

import os
Logger.log.info(os.environ['LOG_PATH'])
Logger.log.info('environment variables loaded!')


from SharedData.Metadata import Metadata

md = Metadata('MASTER/FUT',s3read=True)

from SharedData.SharedData import SharedData

shdata = SharedData('MarketData',s3read=True,s3write=True)
shdata.dataset
data = shdata['MASTER']['D1']
data['m2m']['ES_S01@XCME'].tail(10)

