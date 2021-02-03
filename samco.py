"""
    TO:DO add order taking, tracking, logging function
"""

from uty import uty, JSON


from urllib.parse import urlencode
from datetime import date, datetime, timedelta, time as dtt
from dwn import dwn
from mDwn import mDwn
from logVector import logVector as lv




class samco(dwn, mDwn):

    def __init__(self, Id = None, level = 'debug', **kwargs):

        """
        userName = samco userName
        password = samco passWord
        code     = samco code

        don't disarrage variables variables are position dependent
        
        """

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)


        dwn.__init__(self, Id = self.Id, level = level)

        mDwn.__init__(self, self.Id, level)

        self.brokerName = 'samco'

        self.brokerBase = "https://api.stocknote.com"

        self.logStatus = False

        self.defExg = 'NSE'

        self.broker = JSON.load(self.pathMaster['brokers_'](self.brokerName + '0.json'))



        self.checkList = {}

        self.authHeader = {'Content-Type': 'application/json','Accept': 'application/json'}

        self.reqRoute = {'GET':self.__GET__,
                         'POST':self.__POST__,
                         'PUT':self.__PUT__,
                         'DELETE':self.__DELETE__}


        self.intervalMap = {'1m':1,
                            '5m':5,
                            '10m':10,
                            '15m':15,
                            '30m':30,
                            '1h':60,
                            '1d':None}



    
        self.__runBroker__()

        self.creds = self.__argParse__(kwargs)

        self.__login__()

        



    def getBroker(self):

        """
        function intended for adding broker
        return broker details
        """

        assoc = "samco:getBroker"

        self.mTrace(stamp = self.ts(), assoc = assoc, status = "REQUEST")

        return {self.brokerName:{'exgList':self.allExg, 'ordTypes':self.ordTypes, 'prdTypes':self.prdList, 'quote':self.getQuote}}


    def leaveBroker(self):

        """
        cover method for logging out of broker account
        return None
        """

        assoc = "samco:leaveBroker"

        self.mTrace(stamp = self.ts(), assoc = assoc, status = "REQUEST")

        self.__logout__()






        


    
    def getData(self, index = False, **kwargs):

        """
        function for getting daily and historical data

        symbol = symbol in talk
        exg    = exg in talk
        index = True/False symbol is index or not

        return to base element
        
        """

        assoc = "samco:getData"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", params = kwargs, index = index)

        result = False


        if self.__check__(symbol = kwargs['symbol'], exg = kwargs['exg']):

            interval = self.intervalMap.get(kwargs['interval'])


            if interval != None:


                attr = self.brokerFunc['indData'] if index else self.brokerFunc['symData']

                ssTimes = self.exgTimes(kwargs['exg'])

                kwargs['start'] = datetime.combine(kwargs['start'], ssTimes[0])
                kwargs['stop'] =  datetime.combine(kwargs['stop'], ssTimes[1])

                kwargs['interval'] = interval

                attr['data'] = kwargs

                result = self.__struct__(self.__blTul__(self.reqRoute[attr['method']](attr), attr["parse"]), 'intradayCandleData')
            
                

            else:

                attr = self.brokerFunc['indHData'] if index else self.brokerFunc['symHData']

                del kwargs['interval']

                attr['data'] = kwargs

                result = self.__struct__(self.__blTul__(self.reqRoute[attr['method']](attr), attr["parse"]), 'historicalCandleData')


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", lot = len(result))

            
        return result




        
    
    def getMquote(self, quoteList):

        result, reqParse = {}, {}

        request = []


        for seg in quoteList:

            attr = self.brokerFunc['quoteIndex'] if seg[-1] else self.brokerFunc['quoteSymbol']

            param = self.__argParse__({'symbol':seg[0], 'exg':seg[1]}, attr['toGet'])

            url = attr['endPoint'].format(urlencode(param))

            request.append({'url':url, 'enc':self.enc, 'eCode':[200, 500], 'headers':self.authHeader})

            reqParse[url] = [seg, attr['parse']]


        
        
        resp = self.mRegular(urlList = request, ind = False)
        


        for url, seg in reqParse.items():

            result[seg[0]] = self.__blTul__(JSON.loads(resp[url]), seg[1])



        return result


    

    



    def getQuote(self, index = False, **kwargs):

        """
        function for getting quote details from broker

        symbol = symbol in talk
        exg = exchange in talk
        index = True/False index or not

        return dict of data
        """

        assoc = "samco:getQuote"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", params = kwargs, index = index)

        result = False

        if self.__check__(symbol = kwargs['symbol'], exg = kwargs['exg']):

            attr = self.brokerFunc['quoteIndex'] if index else self.brokerFunc['quoteSymbol']

            attr['data'] = kwargs

            result = self.__blTul__(self.reqRoute[attr['method']](attr), attr['parse'])


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", lot = len(result))

        return result

        

    
            

        


    def __check__(self, **kwargs):

        """
        function for verifying that if a symbol is allowe dwith broker or not

        symbol = symbol to be checked
        exg = symbol from exchange
        
        """

        assoc = "samco:__check__"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", params = kwargs)

        attr = self.brokerFunc['search']

        attr['data'] = kwargs

        stat = self.checkList.get(kwargs['symbol'])

        result = False

        if stat == None or stat.date() < date.today():

            resp = self.reqRoute[attr['method']](attr)

            if resp:

                result = True

                self.checkList[kwargs['symbol']] = self.ts()

        else:

            result = True

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", result = result)


        return result




    def __blTul__(self, _dict_, parseList):

        """
        function for converting responses from broker land to userland variables

        _dict_ = response dict
        parseList = list or params for which data has to parsed

        return userLand Dict
        """

        ts = self.ts()

        assoc = "samco:__blTul__"

        self.mTrace(stamp = ts, assoc = assoc, status = "GET")

        string = str(_dict_)


        for each in parseList:

            rep = self.userPair.get(each)

            if rep != None and rep != "":

                string = string.replace(each, rep)


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS")        


        return eval(string)


        
    


    def __login__(self):

        """
        function for login with broker

        return None
        """

        ts = self.ts()

        assoc = "samco:__login__"

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", log = self.logStatus)

        self.authHeader = {'Content-Type': 'application/json','Accept': 'application/json'}


        attr = self.brokerFunc['login']

        attr['data'] = self.creds

        resp = self.reqRoute[attr['method']](attr)

        self.allExg   = resp['exchangeList']
        self.ordTypes = resp['orderTypeList']
        self.prdList  = resp['productList']



        self.authHeader = headers = {'Accept': 'application/json',
                                      'x-session-token': resp['sessionToken']}

        self.logStatus = True

        print('ginned')

        
        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", log = self.logStatus)                                    

        
    def __logout__(self):

        """
        function for logging out with broker

        return resp dict
        """

        assoc = "samco:__logout__"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", log = self.logStatus)

        attr = self.brokerFunc['logout']

        resp = self.reqRoute[attr['method']](attr)

        self.logStatus = False

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", log = self.logStatus)

        return resp




    def __struct__(self, _dict_, key):

        """
        for parsing out broker data to base data

        _dict_ = broker dict

        key    = key of dict to meat out data
        """

        assoc = "samco:__struct__"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", dictLen = len(_dict_), key = key)

        result = []

        _list_ = _dict_[key]

        for seg in _list_:

            result.append((seg['open'], seg['close'], seg['high'], seg['low'], seg['volume'], seg['stamp']))


        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", lot = len(result), key = key)


        return result

        


    



        
    def __reqParse__(self, _dict_, kwargs):

        """
        function for verfying if response is a success or not if not reassign it

        _dict_ = response  dict

        kwargs = requset dict

        return _dict_
        """

        assoc = "samco:__reqParse__"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET")

        result = False

        if _dict_['status'].lower() == 'success':

            result = _dict_

        else:

            if 'Session Expired' in _dict_['statusMessage']:

                self.__login__()

                self.reqRoute[kwargs['method']](kwargs)

                self.mTrace(stamp = self.ts, assoc = assoc, status = "RETRYING", reason = "sessionExpired")


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS")


        return result



    def __argParse__(self, kwargs, pair = None):

        """
        for converting userLand request to brokerLand

        kwargs = userland dict

        return brokerLand dict

        
        """

        if pair == None:

            pair = self.brokerPair


        result = {}

        for userPair, value in kwargs.items():

            brokerPair = pair.get(userPair)


            if brokerPair != None:

                result[brokerPair] = value


            else:

                result[userPair] = value


        return result




            

        
    def __GET__(self, kwargs):

        """
        GET cover for self.request

        kwargs request Dict

        return json-tested response
        """

        
        url = kwargs['endPoint'].format(urlencode(self.__argParse__(kwargs['data'], kwargs['toGet'])))

        req = JSON.loads(self.regular(url = url, enc = self.enc, eCode = [200, 500], headers = self.authHeader)[url])

        

        return self.__reqParse__(req, kwargs)

    

    def __POST__(self, kwargs):

        """
        POST cover for self.request

        kwargs request Dict

        return json-tested response
        """


        resp = JSON.loads(self.regular(url = kwargs['endPoint'], data = JSON.dumps(kwargs['data']), method = kwargs['method'],
                                       headers = self.authHeader, enc = self.enc)[kwargs['endPoint']])

        

        return self.__reqParse__(resp, kwargs)


    def __PUT__(self, kwargs):

        """
        PUT cover for self.request

        kwargs request Dict

        return json-tested response
        """

        resp = JSON.loads(self.regular(url = kwargs['url'].format(urlencode(kwargs['query'])), data = JSON.dumps(kwargs['data']),
                                       method = kwargs['method'], headers = self.authHeader, enc = self.enc))

        return self.__reqParse__(resp, kwargs)



    


    def __DELETE__(self, kwargs):

        """
        DELETE cover for self.request

        kwargs request Dict

        return json-tested response
        """

        resp = JSON.loads(self.regular(url = kwargs['endPoint'].format(urlencode(kwargs['query'])), method = kwargs['method'],
                                       headers = self.authHeader, enc = self.enc)[kwargs['endPoint']])

        return self.__reqParse__(resp, kwargs)
    


    def __runBroker__(self):

        """
        for creating required variables from json config file

        return None
        """

        self.userPair, self.brokerPair, self.brokerFunc = {}, {}, {}

        for method, funcSeg in self.broker['func'].items():

            for fn, seg in funcSeg.items():

                self.brokerFunc[fn] = seg


        for brokerPair, seg in self.broker['params'].items():

            self.brokerPair[seg['map']] = brokerPair


        for brokerPair, seg in self.broker['resp'].items():

            self.userPair[brokerPair] = seg['map']
            


            

        

            

        

        
    
t = samco(samUser = 'DA34408', samPhrase = 'W@T$@L901', samCode = '1999')

##

#t.__check__(symbol = 'TCS', exg = 'NSE')
print(t.getMquote([('TCS', 'NSE', False)]))
####t.__check__(symbol = 'TCS', exg = 'NSE')
####t.__check__(symbol = 'park', exg = 'NSE')
####t.__check__(symbol = 'KTKBANK', exg = 'NSE')
##
###print(t.getQuote(symbol = 'TCS', exg = 'NSE'))
##print(t.getData(symbol = 'TCS', exg = 'NSE', start = date(2021, 1, 10), stop = date(2021, 1, 21), interval = '1m'))
#t.__logout__()
