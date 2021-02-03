from uty import uty, JSON

from urllib.parse import urlencode

from datetime import datetime, timedelta, time as dt

from dwn import dwn

from mDwn import mDwn

from logVector import logVector as lv



class samco(lv, uty, mDwn):

    def __init__(self, Id = None, level = 'debug', **kwargs):

        uty.__init__(self)

        self.Id = self.getId(Id)

        mDwn.__init__(self, Id = self.Id, level = level)
        dwn.__init__(self, Id = self.Id, level = level)

        self.mRegular([{'url':'https://www.google.com/', 'enc':'utf-8'}])

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)

        self.brokerName = 'samco'

        self.logStatus = False

        self.defExg = 'NSE'

        self.broker = JSON.load(self.pathMaster['brokers_'](self.brokerName + '0.json'))

        self.authHeader = {'Content-Type': 'application/json','Accept': 'application/json'}


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


    def __get__(self, kwargs):

        """
        GET cover for self.request

        kwargs request Dict

        return json-tested response
        """

        
        url = kwargs['endPoint'].format(urlencode(self.__argParse__(kwargs['data'], kwargs['toGet'])))

        req = JSON.loads(self.regular(url = url, enc = self.enc, eCode = [200, 500], headers = self.authHeader)[url])

        

        return self.__reqParse__(req, kwargs)

    

    def __post__(self, kwargs):

        """
        POST cover for self.request

        kwargs request Dict

        return json-tested response
        """

        print(kwargs)


        resp = JSON.loads(self.regular(url = kwargs['endPoint'], data = JSON.dumps(kwargs['data']), method = kwargs['method'],
                                       headers = self.authHeader, enc = self.enc)[kwargs['endPoint']])

        

        return self.__reqParse__(resp, kwargs)


    def __put__(self, kwargs):

        """
        PUT cover for self.request

        kwargs request Dict

        return json-tested response
        """

        resp = JSON.loads(self.regular(url = kwargs['url'].format(urlencode(kwargs['query'])), data = JSON.dumps(kwargs['data']),
                                       method = kwargs['method'], headers = self.authHeader, enc = self.enc))

        return self.__reqParse__(resp, kwargs)



    


    def __delete__(self, kwargs):

        """
        DELETE cover for self.request

        kwargs request Dict

        return json-tested response
        """

        resp = JSON.loads(self.regular(url = kwargs['endPoint'].format(urlencode(kwargs['query'])), method = kwargs['method'],
                                       headers = self.authHeader, enc = self.enc)[kwargs['endPoint']])

        return self.__reqParse__(resp, kwargs)




    





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

        
        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", log = self.logStatus)


        



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





samco(samUser = '', samPhrase = '', samCode = '')
