import matplotlib.pyplot as plt
plt.style.use('default')
import pandas as pd

from SharedData.Logger import Logger
logger = Logger(__file__)

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
plt.show()
fig = plt.figure()
data['ret'][symbol].tail(2520).hist(bins=50)
plt.show()
print(data['m2m'][symbol].tail(25))
print(data['ret'][symbol].tail(25))

from SharedData.Metadata import Metadata

md = Metadata('CURVES/DI1')
md.static

# # PUBLIC SIGNALS
# shsignals = SharedData('Signals')
# shsignals.dataset

# ircurve = shsignals['IR_CURVES']['D1']
# dv01 = ircurve['dv01']
# dv01.loc[dv01.last_valid_index()]

# scnd_implrate = ircurve['scnd_implrate']
# scnd_implrate.loc[scnd_implrate.last_valid_index()]

# # PRIVATE SIGNALS
# shsignals = SharedData('Signals/CARLITO')
# shsignals.dataset

# ircurve = shsignals['IR_CURVES']['D1']
# dv01 = ircurve['dv01']
# dv01.loc[dv01.last_valid_index()]

# scnd_implrate = ircurve['scnd_implrate']
# scnd_implrate.loc[scnd_implrate.last_valid_index()]

# ircurve[pd.Timestamp('2022-02-07')] = df
# ircurve