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

# PRIVATE SIGNALS
shsignals = SharedData('Signals/CARLITO')
shsignals.dataset

ircurve = shsignals['IR_CURVES']['D1']

dt = pd.Timestamp('2022-02-07')
ircurve[dt] = df
ircurve.tags[dt].Write()

from SharedData.Metadata import Metadata

md = Metadata('CARLITO/TEST')
md.static = df
md.save(save_excel=True)