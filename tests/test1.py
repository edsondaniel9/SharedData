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
Logger.log.info(os.environ['DATABASE_FOLDER'])
Logger.log.info('environment variables loaded!')

from SharedData.SharedData import SharedData

shdata = SharedData('MarketData')
shdata.dataset

data = shdata['MASTER']['D1']
symbol = 'DI1_S10@BVMF'
symbol = 'ES_S01@XCME'
fig = plt.figure()
data['m2m'][symbol].tail(2520).plot()
fig = plt.figure()
data['ret'][symbol].tail(2520).hist(bins=50)
print(data['m2m'][symbol].tail(25))
print(data['ret'][symbol].tail(25))

from SharedData.Metadata import Metadata

md = Metadata('MASTER/FUT')
md.static
md.static.columns
md.symbols
md.series

md = Metadata('MASTER/STOCK')
md.static
md.static.columns

md = Metadata('CURVES/DI1')
md.static

shsignals = SharedData('Signals')
shsignals.dataset

ircurve = shsignals['IR_CURVES']['D1']
dv01 = ircurve['dv01']
dv01.loc[dv01.last_valid_index()]

scnd_implrate = ircurve['scnd_implrate']
scnd_implrate.loc[scnd_implrate.last_valid_index()]
