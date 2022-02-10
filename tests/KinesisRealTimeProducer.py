from xbbg import blp
import json
import pandas as pd

from SharedData.Logger import Logger
from SharedData.Metadata import Metadata
from SharedData.SharedDataAWSKinesis import KinesisStreamProducer

logger = Logger(__file__)
Logger.log.info('Starting Kinesis Stream Producer Loop')


futchain = Metadata('BBG/FUT_CHAIN')
futchain.static
coll = Metadata('MASTER/FUT')
coll.static
lvidx = coll.symbols.last_valid_index()
activefuts = coll.symbols.loc[lvidx].dropna()
activefuts = activefuts[[s in futchain.static.index for s in activefuts.values]]
activefuts = pd.DataFrame(activefuts)
activefuts.columns=['symbol']
activefuts['bloombergsymbol'] = futchain.static.loc[activefuts['symbol']]['bloombergsymbol'].values

data = {
    "TICKER": "SIOU2 COMDTY",
    "FIELD": "BID",
    "MKTDATA_EVENT_TYPE": "SUMMARY",
    "MKTDATA_EVENT_SUBTYPE": "INITPAINT",
    "BID": 23.365,
    "ASK": 23.385,
    "LAST_PRICE": 23.349,
    "VOLUME": 6,
    "MID": 23.375,
    "SPREAD_BA": 0.02,
    "IS_DELAYED_STREAM": "false",
    "REALTIME_PERCENT_BID_ASK_SPREAD": 0.08559999999999945,
    "RT_PX_CHG_PCT_1D": 0.5555999875068665
}

producer = KinesisStreamProducer('deepportfolio-real-time','kinesis-logs-write-only')
tickers = activefuts['bloombergsymbol'].values
async for data in blp.live(tickers):        
    with open('data.txt','a+',encoding = 'utf-8') as f:        
        f.write(json.dumps(data, default=str)+'\n')
    producer.produce(data,data['TICKER'])