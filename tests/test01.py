import matplotlib.pyplot as plt
plt.style.use('default')
import pandas as pd

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

from SharedData.SharedData import SharedData

#PUBLIC MARKETDATA
shdata = SharedData('MarketData')
shdata.dataset

cmefut = shdata['CME/FUT']['D1']
df = cmefut[pd.Timestamp('2022-02-03')]
if df.empty:
    Logger.log.error('SharedDataFrame error!')
    raise Exception('SharedData dataframe error!')

data = shdata['MASTER']['D1']
symbol = 'DI1_S10@BVMF'
symbol = 'ES_S01@XCME'
fig = plt.figure()
data['m2m'][symbol].tail(2520).plot()
fig = plt.figure()
data['ret'][symbol].tail(2520).hist(bins=50)
print(data['m2m'][symbol].tail(25))
print(data['ret'][symbol].tail(25))

#PRIVATE MARKETDATA
shdata = SharedData('MarketData/CARLITO')
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

# PUBLIC SIGNALS
shsignals = SharedData('Signals')
shsignals.dataset

ircurve = shsignals['IR_CURVES']['D1']
dv01 = ircurve['dv01']
dv01.loc[dv01.last_valid_index()]

scnd_implrate = ircurve['scnd_implrate']
scnd_implrate.loc[scnd_implrate.last_valid_index()]

# PRIVATE SIGNALS
shsignals = SharedData('Signals/CARLITO')
shsignals.dataset

ircurve = shsignals['IR_CURVES']['D1']
dv01 = ircurve['dv01']
dv01.loc[dv01.last_valid_index()]

scnd_implrate = ircurve['scnd_implrate']
scnd_implrate.loc[scnd_implrate.last_valid_index()]

ircurve[pd.Timestamp('2022-02-07')] = df
ircurve