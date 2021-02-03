from uty import uty, JSON
from logVector import logVector as lv
from datetime import date, datetime

from nse import nse
from db import db
from baseQuery import baseQuery as bq

class nseApi( nse, db, bq ):


    def __init__( self, Id = None, level = 'debug' ):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)

        nse.__init__(self, Id = self.Id, level = level)

        bq.__init__(self, Id = self.Id, level = level)

        self.db = db(Id = self.Id, level = level)


        self.USER = 'NSEAPI'

        self.exg = "NSE"

        self.exgId = self.nseId()




    


    def pullIndices(self, upd = False):

        """
        upd = True/False for update of content
        """

        assoc = "nseApi:pullIndexes"
        ts = self.ts()

        result = False

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', upd = upd)

        if upd:

            #checking for update switch
            
            self.pushIndices()


        
        with self.db as cu:

            q = "select * from `indStore`;"

            result = cu.execute(query = q, func = lambda elem: self.bTj(elem = elem, pos = [2]))[q]


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = 'SUCCESS', lot = len(result))


        return result


        



    def pushIndices(self):

        rawData = self.getIndices(content = True)

        #rawData = JSON.load('test.json')

        symStore, indStore = [], []

        for secInd, symSeg in rawData.items():

            isins = []

            for symInfo in symSeg:

                isins.append(symInfo[4])

                symInfo = list(symInfo)

                symInfo.append(self.exgId)

                symStore.append(tuple(symInfo))

            indStore.append((secInd[0], secInd[1], JSON.dumps(isins), self.dbId(), self.exgId))


        symStore = str(symStore)[1:-1]
        indStore = str(indStore)[1:-1]

        with self.db as cu:

            cu.execute(f"replace into `symStore` (`symCmp`, `symSec`, `sym`, `symSeg`, `symId`, `ESId`) values {symStore};")
            cu.cnx.commit()
            cu.execute(f"replace into `indStore` (`indSec`, `ind`, `indCont`, `indId`, `EIId`) values {indStore};")
            cu.cnx.commit()




    def pullHolidays(self, start, stop = date.today().year, upd = False):

        """
        checks data, pulls data and retrives data for the request

        start = start year
        stop  = stop year

        return list of dates which are holidays
        """

        assoc = "nseApi:pullHoliday"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', start = start, stop = stop)

        event, hNse, result = [], [], False


        #statusCheck

        if not upd:

            #checking for update

            res = self.holidayStatus(start = start, stop = stop)
            

        if upd or res:

            #pushingHolidays

            self.pushHolidays(start = start, stop = stop)

            
        #dataRetrival

        with self.db as cu:

            q = f'select `hStamp` from `hNse`;'

            result = cu.execute(q)[q]

        
        self.mTrace(stamp = self.td(ts), assoc = assoc, status = 'SUCCESS', lot = len(result))


        return result




    def pushHolidays(self, start, stop = date.today().year):

        assoc = "nseApi:pushHolidays"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', start = res['start'], stop = res['stop'])

        #nsePull

        rawHolidays = self.getHolidays(start = res['start'], stop = res['stop'])

        #dataAdjustments for both databases

        for each in rawHolidays:

            each.append(self.USER)
            
            each = tuple(each)

            event.append(each)

            hNse.append((each[0], ))


        event = str(event)[1:-1]

        hNse = str(hNse)[1:-1]

        #dataInsertion

        with self.db as cu:

            cu.execute(f'replace into `event` (`eStamp`, `eEvent`, `eUser`) values {event};')
            cu.execute(f'replace into `hNse` values {hNse};')

            cu.cnx.commit()


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = 'SUCCESS', start = res['start'], stop = res['stop'])




    def nseId(self):

        with self.db as cu:

            q = f'select `exgId` from `exgStore` where `exg` = "{self.exg}"'

            res = cu.execute(q)[q]

            if len(res):

                result = res[0]

            else:

                
                cu.execute(f"replace into `exgStore` (`exg`, `exgId`) values ('{self.exg}', '{self.dbId()}')")

                cu.cnx.commit()

                result = cu.execute(q)[q]


        return result



        



        
#nseApi().pullIndices()
