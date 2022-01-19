import userconfig
import SharedData.Logger as Logger
Logger.setLogger(__file__,userconfig)
userconfig.logger.info('logger test')

from SharedData.SharedData import SharedData
from SharedData.Metadata import Metadata

md = Metadata('MASTER/FUT',userconfig)
md.save()


shdata = SharedData(userconfig,'MarketData')
shdata.dataset
data = shdata['MASTER']['D1']
data['m2m']