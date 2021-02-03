import time
from logVector import logVector as lv
from datetime import datetime, date, timedelta
from uty import uty, JSON
from mDwn import mDwn



class yahoo( mDwn ):


    def __init__(self, Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)


        mDwn.__init__(self, self.Id, level)


        self.lfd = {'1m':29,
                    '5m':59,
                    '15m':59,
                    '1h':59,
                    '1d':False,
                    '1wk':False,
                    '1mo':False}


        self.availInterval = lambda: list(self.lfd.keys())

        self.exg = "NSE"

        self.statUrl = 'https://query1.finance.yahoo.com/v8/finance/chart/\
                       {}.{}?symbol={}.{}\
                       &period1={}\
                       &period2={}\
                       &interval={}\
                       &includePrePost=true\
                       &events=div%7Csplit%7Cearn\
                       &lang=en-IN\
                       &region=IN\
                       &crumb=undefined\
                       &corsDomain=in.finance.yahoo.com'




        
    def getData(self, symbol, start, stop, interval, exg = None):

        """
        function for pulling data from yahoo
        symbol   = symbol in talk
        start    = to get from
        stop     = to get to
        interval = interval
        exg      = exchange in talk

        return:list of quotes
        """

        ts = self.ts()

        assoc = "yahoo:getData"

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', symbol = symbol, start = start, stop = stop, interval = interval, exg = exg)

        result = False

        stop = self.__getLfd__(interval, stop)


        exg = self.exg if exg == None else exg

        self.ssTimes = self.exgTimes(exg)

        getList = self.__respGranes__(symbol = symbol, start = start,
                            stop = stop, interval = interval,
                            exg = exg)


        start = int(time.mktime(datetime.combine(start, self.ssTimes[0]).timetuple()))
        stop  = int(time.mktime(datetime.combine(stop,  self.ssTimes[1]).timetuple()))


        result = self.__struct__(self.mRegular(getList), start, stop)
        
        self.mTrace(stamp = self.td(ts), assoc = assoc, status = 'SUCCESS', lot = len(result))

        return result

        


    def __struct__(self, data, start, stop):

        """
        data  = json stringed data
        start = start date in int
        stop  = stop date in int

        return: to base data
        """

        assoc = "yahoo:__struct__"

        store = []

        for rawData in data.values():

            rawData = rawData.replace('null', '0.0')

            res = JSON.loads(rawData)

            error = res['chart']['error']

            if error == 0.0:

                root = res['chart']['result'][0]

                if 'timestamp' in root.keys():

                    quote = root['indicators']['quote'][0]
                    timeStamp = root['timestamp']

                    for itr, each in enumerate(timeStamp):

                        if each >= start and each <= stop:
                            

                            store.append((quote['open'][itr],
                                          quote['close'][itr],
                                          quote['high'][itr],
                                          quote['low'][itr],
                                          quote['volume'][itr],
                                          time.strftime('%Y-%m-%d %H:%M:', time.localtime(each)) + '00'))


##                            store.append((self.dec(quote['open'][itr]),
##                                          self.dec(quote['close'][itr]),
##                                          self.dec(quote['high'][itr]),
##                                          self.dec(quote['low'][itr]),
##                                          self.dec(quote['volume'][itr]),
##                                          time.strftime('%Y-%m-%d %H:%M:', time.localtime(each)) + '00'))

                            

            else:

                self.lv.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", jsonRead = "yahoo data not readable")


                

                    

        return store


        

    def __respGranes__(self, symbol, start, stop, interval, exg):

        """
        granualating and url formation data
        start = start date
        stop  = stop date
        interval = interval
        exg = exg in question

        return list of dict of requests
        """


        result = []

        counter = start

        yExg = self.yahooExg(exg)

        while start <= stop:

            if start.weekday() == 0:

                counter = start

            elif start.weekday() == 4 or start == stop:
                
                temp = [int(time.mktime(datetime.combine(counter, self.ssTimes[0]).timetuple())),
                        int(time.mktime(datetime.combine(start,   self.ssTimes[1]).timetuple()))]

                result.append({'url':self.statUrl.format(symbol, yExg, symbol, yExg,
                                                  temp[0], temp[1], interval).replace(' ', ''),
                               'enc':self.enc})

            

            start += timedelta(days = 1)


        return result


        

            

            
        
    def __getLfd__(self, interval, stamp):

        """
        interval = interval of data
        stamp    = stop stamp

        return   = allowed last fetch date
        """

        assoc = "yahoo:__getLfd__"

        limit = self.lfd.get(interval)

        result = stamp

        if limit != None and limit != False:
            
            lastFetch = date.today() - timedelta(days = limit)

            if stamp < lastFetch:

                result = lastFetch

                self.lv.mTrace(stamp = self.ts(), assoc = assoc, status = "PUT", lastFetch = lastFetch)



        return result


##
##
##t = yahoo().getData(symbol = 'KTKBANK', start = date(2021, 1, 1),
##                      stop = date(2021, 1, 17), interval = '1m')
##
##with open('test.json', 'w') as f:
##
##    JSON.dump(t, f, indent = 4)
