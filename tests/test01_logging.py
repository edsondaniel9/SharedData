import matplotlib.pyplot as plt
plt.style.use('default')
import pandas as pd

# import sys
# print(sys.path)
print(__file__)
import os
print(os.environ['PYTHONPATH'])

from SharedData.Logger import Logger
logger = Logger(__file__)

import numpy as np
message = 'Logging test %2.10f' % (np.random.rand())
Logger.log.info(message)
dflogs = logger.readLogs()
if (dflogs.iloc[-1]['message'] == message):
    Logger.log.info('logging works!')
else:
    raise Exception('SharedData logging error!')

import os
Logger.log.info(os.environ['DATABASE_FOLDER'])
Logger.log.info('environment variables loaded!')
