import matplotlib.pyplot as plt
plt.style.use('default')

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


from SharedData.SharedData import SharedData

shdata = SharedData('MarketData',s3read=True,s3write=False)
shdata.dataset

data = shdata['MASTER']['D1']
data['m2m']['ES_S01@XCME'].tail(2520).plot()
data['ret']['ES_S01@XCME'].tail(2520).hist(bins=50)


from SharedData.Metadata import Metadata

md = Metadata('MASTER/FUT',s3read=False,s3write=False)
md.static
md.static.columns
md.symbols
md.series

md = Metadata('MASTER/STOCK',s3read=False,s3write=False)
md.static
md.static.columns

