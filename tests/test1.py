import userconfig
import SharedData.Logger as Logger
Logger.setLogger(__file__,userconfig)
userconfig.logger.info('logger test')

from SharedData.SharedData import SharedData

shdata = SharedData(userconfig,'MarketData')
shdata.dataset