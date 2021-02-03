from uty import uty
from logVector import logVector as lv
from datetime import date
from yahoo import yahoo
from db import db

class yahooUpd( yahoo ):

    #to:do add functionalty for different exchanges
    #only supports nse for now


    def __init__(self, Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
            logFile = self.pathMaster['logs'],
            Id = self.Id,
            level = level)

        


        yahoo.__init__(self, level = level)

        self.db = db(Id = self.Id, level = level)

        self.availIntervals = self.availInterval()

        self.tabName = lambda symbol, interval: f'{symbol}_{interval}'

        self.exg = "NSE"


    def updData(self, symbol, start, stop, interval, upd = False):

        if interval in self.availIntervals:

            tabName = self.tabName(symbol, interval)


            with self.db as cu:

                q = f"""create table if not exists `intData`.{tabName} like `intSamp`;"""

                cu.execute(q)

                

                if upd:

                    rawData = self.getData(symbol = symbol, start = start, stop = stop, interval = interval, exg = self.exg)

                    q = f"""replace into `intData`.{tabName} (`intOpen`, `intClose`, `intHigh`, `intLow`, `intVolume`, `intStamp`) values {str(rawData)[1:-1]};"""

                    cu.execute(q)

                    cu.cnx.commit()







yahooUpd().updData(symbol = "TCS", start = date.today().replace(day = 15, month = 1), stop = date.today(), interval = "1m", upd = True)

