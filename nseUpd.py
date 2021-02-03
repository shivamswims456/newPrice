from uty import uty, JSON
from logVector import logVector as lv
from datetime import datetime, date

from nse import nse
from db import db
from baseQuery import baseQuery as bq
from redisClient import redisClient


class nseUpd( nse, db, bq, redisClient ):

    """

        upd should be True for procurement function to update there respective destination and
        can be allowed True for partialStatic function if you want to repdresh the database
    """


    def __init__(self, allowedSegs = ['EQ'], Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)

        nse.__init__(self, Id = self.Id, level = level)

        bq.__init__(self, Id = self.Id, level = level)

        redisClient.__init__(self, Id = self.Id, flush = True, db = "quote")

        self.db = db(Id = self.Id, level = level)

        


        self.USER = 'NSEAPI'

        self.exg = "NSE"

        self.exgId = self.nseId()

        self.allowedSegs = allowedSegs



    def updQuote(self, symbolList, upd = False):

        """

        function for pushing quotes to redis
        symbolList = list of segs for which quote is required

        return None
        """

        assoc = "nseUpd:updQuote"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", upd = upd , symbolList = symbolList)

        
        if upd:

            self.rSet(self.getQuote(symbolList))

            
        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "GET", upd = upd)
    

    def updInfo(self, symbolList, upd = False):

        """
        function for updating info symbols in symbolTable

        symbolList = list of symbols
        
        """

        assoc = "nseUpd:updSymbols"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", upd = upd, symbolList = symbolList)

        if upd:

            result = self.getInfo(symbolList = symbolList)

            qData = []


            for value in result.values():

                value.append(self.exgId)

                qData.append(tuple(value))

            qData = str(qData)[1:-1]

            q = f"""insert into `symStore` (`symCmp`, `symSec`, `sym`, `symSeg`, `symCap`, `symId`, `symStat`, `ESId`)
                    values {qData} on duplicate key update
                    `symCmp` = values(`symCmp`), `symSec`  = values(`symSec`), `sym`   = values(`sym`),
                    `symSeg` = values(`symSeg`), `symCap`  = values(`symCap`), `symId` = values(`symId`),
                    `ESId`   = values(`ESId`),   `symStat` = values(`symStat`);"""


            with self.db as cu:

                cu.execute(q)

                cu.cnx.commit()

            
            

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", lot = len(result))
    

    def updSymbols(self, upd = False):

        """

        this function refreshes some of the informations of all symbols on Nse
        to symbolTable is lot faster then updInfo function
        
        """

        assoc = "nseUpd:updSymbols"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", upd = upd)

        if upd:

            finalData = []

            rawData = self.getSymbols()

            for seg in rawData:

                seg.append(self.exgId)

                finalData.append(tuple(seg))


            finalData = str(finalData)[1:-1]


            with self.db as cu:

                q = f"""insert into `symStore` (`symCmp`, `sym`, `symSeg`, `symId`, `ESId`) values {finalData}
                        on duplicate key update
                        `symCmp` = values(`symCmp`),
                        `sym`    = values(`sym`),
                        `symSeg` = values(`symSeg`),
                        `ESId`   = values(`ESId`);"""
                
                cu.execute(q)

                cu.cnx.commit()

                
                

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", upd = upd)

                



    def updFins(self, start, stop, upd = False, allowedSegs = None):

        """
        updates fin Table with bhav and volt files infos, create tables
        for them if not present like the sample present in the base

        start = startData
        stop = stopDate

        allowedSegs = ['EQ'] default
        """

        assoc = "nseUpd:updFins"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", upd = upd)

        self.updHolidays(start = start.year, stop = stop.year)

        allowedSegs = self.allowedSegs if allowedSegs == None else allowedSegs


        #holidayUpdateHasTobeMade

        hlSet = self.__exgHolidays__(start = start, stop = stop)

        reqList = self.NseFinStat(start = start, stop = stop, hlSet = self.__exgHolidays__(start = start, stop = stop))

        if upd:

            reqList = sorted(list(self.stamps(start = start, stop = stop) - hlSet), sorted = True)
        
        
        rawData = self.getFinData(reqList = reqList, allowedSegs = allowedSegs)

     
        with db(asy = True) as cu:

            q = [{'query':f'create table if not exists `finData`.{symbol} like `finSamp`'} for symbol in rawData.keys()]

            cu.tranSact(q)


            q = [{'query':f"""insert into `finData`.{symbol}
                              (`finStamp`,  `finPclose`, `finOpen`,   `finHigh`,   `finLow`,    `finClose`,
                               `finVolume`, `finTrnOvr`, `finTrdMde`, `finDelQty`, `finDelPer`, `finLogRet`,
                               `finPreVot`, `finCurVot`, `finAnnVot`)
                               
                              values {str(data)[1:-1]}
                              
                              on duplicate key update
                              (`finStamp`  = values(`finStamp`),  `finPclose` = values(`finPclose`), `finOpen`   = values(`finOpen`),
                               `finHigh`   = values(`finHigh`),   `finLow`    = values(`finLow`),    `finClose`  = values(`finClose`),
                               `finVolume` = values(`finVolume`), `finTrnOvr` = values(`finTrnOvr`), `finTrdMde` = values(`finTrdMde`),
                               `finDelQty` = values(`finDelQty`), `finDelPer` = values(`finDelPer`), `finLogRet` = values(`finLogRet`),
                               `finPreVot` = values(`finPreVot`), `finCurVot` = values(`finCurVot`), `finAnnVot` = values(`finAnnVot`);"""}\

                 for symbol, data in rawData.items()]
            
            cu.tranSact(q)
            

        
        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", upd = upd)

        

        

    

    def updIndices(self, upd = False):

        """
        updates indexList and index Content List
        creates table for index if not present
        """

        """
        to make consistency with all status driven function upd has been added
        """

        assoc = "nseUpd:updIndices"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", upd = upd)

        symStore, indStore = [], []

        if upd:

            rawData = self.getIndices(content = True)

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

                cu.execute(f"""insert into `symStore` (`symCmp`, `symSec`, `sym`, `symSeg`, `symId`, `ESId`)
                               values {symStore}
                               on duplicate key update
                               `symCmp` = values(`symCmp`),
                               `symSec` = values(`symSec`),
                               `sym`    = values(`sym`),
                               `symSeg` = values(`symSeg`),
                               `symId`  = values(`symId`),
                               `ESId`   = values(`ESId`));""")

                
                cu.cnx.commit()
                
                cu.execute(f"""insert into `indStore` (`indSec`, `ind`, `indCont`, `indId`, `EIId`)
                               values {indStore}
                               on duplicate key update
                               `indSec   = values(`indSec`),
                               `ind`     = values(`ind`),
                               `indCont` = values(`indCont`),
                               `intId`   = values (`indId`),
                               `EIId`    = values(`EIId`);""")
                    
                cu.cnx.commit()




        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "GET", upd = upd)

        


    def updHolidays(self, start, stop = date.today().year, upd = False):

        """
        updates holiday Table and event table with holiday dates
        """

        assoc  = "nseUpd:updHolidays"
        
        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", start = start, stop = stop, upd = upd)

        event, hNse, = [], []

        
        if not upd:

            res = self.holidayStatus(start = start, stop = stop)


            

        if upd or res:

            #parametersUpdate

            start, stop = res['start'], res['stop']

            #nsePull

            rawHolidays = self.getHolidays(start = start, stop = stop)

            for each in rawHolidays:

                each.append(self.USER)
                
                each = tuple(each)

                event.append(each)

                hNse.append((each[0], ))


            event = str(event)[1:-1]

            hNse = str(hNse)[1:-1]


            with self.db as cu:

                cu.execute(f'replace into `event` (`eStamp`, `eEvent`, `eUser`) values {event};')
                cu.execute(f'replace into `hNse` values {hNse};')
                cu.cnx.commit()


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", upd = upd)

            


        

            

            


    def nseId(self):

        """
        fetched down nse id from id table
        if not available allocates one
        """

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


    def __exgHolidays__(self, start, stop):

        """
        inside function for getting holidays for removing fetch of holidays for
        date dependent file
        
        """

        assoc = "nseUpd.__exgHolidays__"

        ts = self.ts()

        result = []

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", start = start, stop = stop)

        with self.db as cu:

            q = f'select `hStamp` from `hNse` where `hStamp` between "{start}" and "{stop}";'


            result = cu.execute(q, func = lambda _: set(each[0] for each in _))[q]


            
        self.mTrace(stamp = ts, assoc = assoc, status = "SUCESS", start = start, lot = len(result))

        return result


            

#print(nseUpd().updFins(start = date.today().replace(day = 1), stop = date.today()))

#nseUpd().updHolidays(start = 2021)

#nseUpd().updQuote([('TCS', 'NSE', False)], True)

#nseUpd().updSymbols(True)

#nseUpd().updInfo(symbolList = ['TCS'], upd = True)
