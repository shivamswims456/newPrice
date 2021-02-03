from uty import uty, JSON
from logVector import logVector as lv

import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from copy import deepcopy

from dwn import dwn
from mDwn import mDwn
from dwnX import dwnX






class nseReq( uty, lv ):


    #TO-DO add index support

    def __init__(self, Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        self.level = level

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)

        

        

        self.masterUrl = 'https://www.nseindia.com/get-quotes/equity?symbol=TCS'

        self.header = {'Host':'www.nseindia.com', 
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
                        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 
                        'Accept-Language':'en-US,en;q=0.5', 
                        'Accept-Encoding':'gzip, deflate, br',
                        'DNT':'1', 
                        'Connection':'keep-alive', 
                        'Upgrade-Insecure-Requests':'1',
                        'Pragma':'no-cache',
                        'Cache-Control':'no-cache'}


        self.result = {}

        


    def __default__(self):


        """
        default login type of request for nse to fetch cookie protected data

        sets self.req_cookies for further request
        """

        assoc = "nseReq:__default__"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET")

        with dwnX(name = 'ini', Id = self.Id, level = self.level, headers = self.header) as dwn:

            resp = dwn.pull(url = self.masterUrl)[self.masterUrl]


        #acquiring cookies
        self.req_cookies = {each:resp.cookies[each] for each in resp.cookies}

        
        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS")



    def cPull(self, reqList):

        """
        reqList = list of requests to get url in form [{'kwargs':{'url':''}, ...}]

        return  {url:encoded text response}
        """

        assoc = "nseReq:cPull"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", reqList = reqList)

        self.__default__()          #cookie setting

        _200, repList = set(), []

        
        with dwnX(name = 'info', Id = self.Id, level = self.level, asy = True, headers = self.header, cookies = self.req_cookies) as dwn:

            resBlob = dwn.pull(reqList)



        for url, res in resBlob.items():

            if res.status_code == 200:

                #sacnning for success resonses

                self.result[url] = res.text

                _200.add(url)

            elif res.status_code == 401:

                #reRepeting request for all unsucessful response

                repList = list(resBlob.keys() - _200)

                break


        if len(repList) > 0:

            #replist check

            self.__fetch__(repList)

                

        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", lot = len(self.result))
            
        return self.result





        
        


class nse( mDwn, dwn, nseReq):


    def __init__(self, allowedSegs = ['EQ'], Id = None, level = 'debug'):

        uty.__init__(self)
        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
            logFile = self.pathMaster['logs'],
            Id = self.Id,
            level = level)

        

        dwn.__init__(self, self.Id, level)
        mDwn.__init__(self, self.Id, level)
        nseReq.__init__(self, Id = Id, level = level)

        self.allowedSegs = allowedSegs



    def getInfo(self, symbolList):

        """
        function for getting data associated wit ha symbol
        symbolList = list of symbols for which data is required

        return {symbol:data}
        """

        statUrl = "https://www.nseindia.com/api/quote-equity?symbol={}"

        urlMap, reqList = {}, []

        result = {}

        for sym in symbolList:

            url = statUrl.format(sym)

            reqList.append({'kwargs':{'url':url}, 'eCode':[200, 401]})

            urlMap[url] = sym


        for url, text in self.cPull(reqList).items():

            _ = JSON.loads(text)

            _ = [_['info']["companyName"],
                 _['info']["industry"],
                 _['info']["symbol"],
                 JSON.dumps(_['info']["activeSeries"]),
                 _["securityInfo"]["issuedCap"],
                 _['info']["isin"],
                 1 if _['metadata']['status'].lower() == "listed" else 0]

            result[urlMap[url]] = _



        return result

            



    def getQuote(self, symbolSeg):

        """
        function for getting quote details from nse
        symbolList list of segs for which symbol is requires [(symbol, exg, index:True/False)]

        return {symbol:data}
        """

        statUrl = "https://www.nseindia.com/api/quote-equity?symbol={}&section=trade_info"

        urlMap, reqList = {}, []

        result = {}

        _quoteReplace_ = {'totalBuyQuantity':'tbq',
                          'totalSellQuantity':'tsq',
                          'quantity':'qty',
                          'bid':'bidList',
                          'ask':'askList',
                          'totalTradedVolume':'ttVol',
                          'totalTradedValue':'ttVal',
                          'deliveryToTradedQuantity':'delPer'}

        
        for seg in symbolSeg:

            url = statUrl.format(seg[0])

            reqList.append({'kwargs':{'url':url}, 'eCode':[200, 401]})

            urlMap[url] = seg[0]

        
        for url, text in self.cPull(reqList).items():

            for bl, ul in _quoteReplace_.items():

                text = text.replace(bl, ul)


            result[urlMap[url]] = JSON.loads(text)


        return result

            

    


    def getSymbols(self):

        """
        funcion for getting all symbols currently active on nse

        return [(data), (data)]
        
        """

        assoc = "nse:getSymbols"

        statUrl = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET")

        rawCsv = self.regular(url = statUrl, enc = self.enc)[statUrl]

        symHeads = ['NAME OF COMPANY', 'SYMBOL', 'SERIES', 'ISIN NUMBER']

        baseElem = self.csvBase(rawCsv, headers = symHeads, ff = lambda _ : [_[0], self.splString(_[1]), JSON.dumps([_[2]]), _[3]])

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", lot = len(baseElem))

        return baseElem

        
    
    def getIndices(self, content = False):

        """
        content = False/True for index contents
        Only those indexes will be returned where csv file of its content are present

        return {symbol:data}
        """

        assoc = 'nse:getIndex'
        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', content = content)

        statUrl = 'https://www.niftyindices.com'

        indexHead = ['Company Name', 'Industry', 'Symbol', 'Series', 'ISIN Code']

        indexCollections = dict()

        csvList, getList = [], []

        csvMap, urlMap = {}, {}

        raw = self.regular(url = statUrl, enc = 'utf-8')[statUrl]

        soup = self.bsp(raw, 'lxml')

        for ul in soup.find_all('ul', class_ = 'tabinsaidmenu'):

            for link in ul.find_all('a'):

                href = link.get('href')

                if href != '#':

                    getUrl = statUrl + href

                    indexName = (getUrl.split('/')[5], self.splString(link.string).upper())

                    getList.append({'url':getUrl, 'enc':self.enc})

                    urlMap[getUrl] = indexName

                    indexCollections[indexName] = []


                    


        if content:
            

            for url, rawPage in self.mRegular(getList, True).items():

                indexName = urlMap[url]

                indexCollections[indexName] = []
                
                rawPage = self.regular(url = url, enc = self.enc)[url]

                csvLink = [link for link in re.findall(r'"(.*?)"', rawPage) if '.csv' in link]
        
                if len(csvLink) != 0:

                    csvLink = csvLink[0]

                    repIndex = csvLink.index('Index') - 1

                    csvUrl = statUrl + csvLink[repIndex:]

                    csvList.append({'url' : csvUrl, 'enc' : self.enc})

                    csvMap[csvUrl] = indexName


                        



            for url, rawCsv in self.mRegular(csvList, True).items():
                

                if isinstance(rawCsv, str):

                    indexCollections[csvMap[url]] = self.csvBase(rawCsv, headers = indexHead, ff = lambda _ : [_[0], _[1], self.splString(_[2]), _[3], _[4]])

                else:


                    indexCollections[csvMap[url]] = []

                    self.mTrace(stamp = self.td, assoc = assoc, status = "INFO", reason = f'Can\'t, read response from url => {url}')



        self.mTrace(stamp = self.td(ts), assoc = assoc, status = 'SUCCESS', lot = len(indexCollections))

        return indexCollections


    def getFinData(self, reqList = [], allowedSegs = None):

        """
        function for getting combined information from secBhav and voltFile

        reqList = [dates]

        return {symbol:[[data], [data]]}
        """

        assoc = "nse:getFinData"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', reqList = reqList, allowedSegs = allowedSegs)

        allowedSegs = self.allowedSegs if allowedSegs == None else allowedSegs

        bhavCopy = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_{}.csv'
        voltCopy = 'https://archives.nseindia.com/archives/nsccl/volt/CMVOLT_{}.CSV'

        getVolt, getBhav = [], []

        obj, logObj = dict(), defaultdict(list)

        urlMap = {}

        headBhav = ['SYMBOL', 'SERIES', 'DATE1', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE',\
                    'TTL_TRD_QNTY', 'TURNOVER_LACS', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']
        headVolt = ['Date', 'Symbol', 'Underlying Log Returns (C) = LN(A/B)','Previous Day Underlying Volatility (D)',\
                    'Current Day Underlying Daily Volatility (E) = Sqrt(0.995*D*D + 0.005*C*C)',\
                    'Underlying Annualised Volatility (F) = E*Sqrt(365)']

        
        bf = lambda temp: obj.update({self.splString(temp[0]):[datetime.strptime(temp[2], '%d-%b-%Y').strftime('%Y-%m-%d')] + temp[3:]}) if temp[1].upper() in allowedSegs else None

        vf = lambda temp: logObj[temp[1]].append(tuple(obj[temp[1]] + temp[2:])) if temp[1] in obj.keys() else None

        #for any none respose csvTBase will not register the row



        for Date in reqList:

            Date = Date.strftime('%d%m%Y')

            bhavUrl = bhavCopy.format(Date)
            voltUrl = voltCopy.format(Date)

            urlMap[bhavUrl] = voltUrl
            
            
            getBhav.append({'url' : bhavUrl, 'enc' : self.enc})
            getVolt.append({'url' : voltUrl, 'enc' : self.enc})
                    


            

        voltObj = self.mRegular(getVolt, True)

        
        for url, string in self.mRegular(getBhav, True).items():


            self.csvBase(string = string, headers = headBhav, ff = bf, sub = 'null')

            self.csvBase(string = voltObj[urlMap[url]], headers = headVolt, ff = vf, sub = 'null')

            #logObj has been passed through vf fubction for result preperation


        logObj = dict(logObj)


        return logObj


            


        
        

        


            

        

        

        

    def getHolidays(self, start, stop = date.today().year):

        """
        function for getting holidays from desired year to desired year

        start = startYear
        stop = stopYear

        return [(data), (data)]
        """

        assoc = "nse:getHoliday"
        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', start = start, stop = stop)

        start = date(start, 1, 1)
        stop  = date(stop, 12, 31)


        trdUrl = 'https://www1.nseindia.com/global/content/market_timings_holidays/market_timings_holidays.jsp?\
                   pageName=0\
                   &dateRange=\
                   &fromDate={}\
                   &toDate={}\
                   &tabActive=trading\
                   &load=false'


        trdUrl = trdUrl.format(start.strftime('%d-%m-%Y'), stop.strftime('%d-%m-%Y')).replace(' ', '')

        data = self.regular(url = trdUrl, enc = self.enc)[trdUrl]

        soup = self.bsp(data, 'lxml').find_all('td')

        elems = [elems.text for elems in soup]

        n = 4

        elems = [[datetime.strptime(elems[i:i + n][1], '%d-%b-%Y').strftime('%Y-%m-%d')]\
                 + [elems[i:i + n][-1]]\
                 for i in range(0, len(elems), n)]

        elems += self.__sanSat__(start, stop)

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = 'SUCCESS', lot = len(elems))

        return elems


    
                               


    def __sanSat__(self, start, stop):



        result = list()
        
        start += timedelta(days = 6- start.weekday())

        while start.year <= stop.year:

            result.append([str(start), 'Sunday'])

            sat = deepcopy(start)

            sat -= timedelta(days = 1)

            if sat.year <= stop.year:

                result.append([str(sat), 'Saturday'])

            start += timedelta(days = 7)


        return result



##t = nse().getHolidays(start = 2021)
##
##
##
    
##t = nse().getSymbols()
##
###t = nse().getFinData(reqList = [date(2021, 1, 29), date(2021, 1, 28)])
##
##with open('test.json', 'wt') as f:
##
##    JSON.dump(t, f, indent = 4)
##
##

#print(nse().getQuote([('TCS', "NSE", False), ('INFY', "NSE", False)]))

#print(nse().getInfo(['TCS', 'INFY']))

#print(nse()(['TCS', 'INFY']))


#print(nse().getIndex(True))

    

        
