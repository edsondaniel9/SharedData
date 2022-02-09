from SharedData import Metadata
from xbbg import blp
import json
import pandas as pd

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

#tickers = ['ESA INDEX','CLA COMDTY']
tickers = activefuts['bloombergsymbol'].values
async for data in blp.live(tickers):
    data['TICKER']
    with open('data.txt','a+',encoding = 'utf-8') as f:        
        f.write(json.dumps(data, default=str)+'\n')