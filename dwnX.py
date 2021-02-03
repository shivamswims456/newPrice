from uty import uty
from logVector import logVector as lv


import httpx, asyncio


STORE = {}


class dwnXasync( uty, lv, httpx.AsyncClient ):

    global STORE

    def __init__(self, name, Id = None, level = 'debug', *args, **kwargs):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)


        self.name = name
        self.state = None

        httpx.AsyncClient.__init__(self, *args, **kwargs)

        self.route = {"GET":self.get,
                      "SET":self.post,
                      "PUT":self.put,
                      "DELETE":self.delete}


    def __get__(self, obj, objType):

        return self

    async def __qs__(self, **kwargs):

        assoc = "dwnXasync:__qs__"

        self.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", reason = "method nnot in register")



    async def __fetch__(self, queryList):

        
        tasks = []
        self.result = {}


        for seg in queryList:


            data =  {'method':"GET",
                     'kwargs':None,
                     "eCode":[200]}

            data.update(seg)

            try:

                resp = await self.route.get(data.get('method'), self.__qs__)(**data['kwargs'])

                if resp.status_code in data['eCode']:
                
                    self.result[seg['kwargs']['url']] = resp

                else:

                    self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'status code not matched')


            except httpx.ConnectError:

                self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'url not accessible')



            
    def rem(self):

        assoc = "dwnXasync:rem"

        self.mTrace(stamp = self.ts(), assoc = assoc, status = "INFO", name = self.name)

        asyncio.gather(STORE[self.name].aclose())

        del STORE[self.name]


    def pull(self, queryList):

        assoc = "dwnXasync:pull"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", queryList = queryList)

        asyncio.run(self.__fetch__(queryList))

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", lot = len(self.result))

        return self.result

        
        

            

            

        

        



class dwnXsync( uty, lv, httpx.Client ):

    global STORE

    def __init__(self, name, Id = None, level = 'level', *args, **kwargs):

        
        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)


        

        self.name = name

        self.state = None

        httpx.Client.__init__(self, *args, **kwargs)

        self.route = {"GET":self.get,
                      "POST":self.post,
                      "PUT":self.put,
                      "DELETE":self.delete}


        self.getStore = lambda: STORE



    def __get__(self, obj, objType):

        return self


    def __qs__(self, **kwargs):

        assoc = "dwnXsync:__qs__"

        self.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", reason = "method not in register")


    def rem(self):

        assoc = "dwnXsync:rem"

        self.mTrace(stamp = self.ts(), assoc = assoc, status = "INFO", name = self.name)

        STORE[self.name].__exit__()

        del STORE[self.name]



    def pull(self, method = "GET", eCode = [200], **kwargs):

        assoc = "dwnXsync:pull"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", method = method, kwargs = kwargs)

        try:
        
            met = self.route.get(method, self.__qs__)

            self.state = met(**kwargs)

            result = {kwargs['url']:self.state}


            if self.state.status_code not in eCode:

                self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'status code not matched')

                result = False


        except httpx.ConnectError:

            self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'url not accessible')


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", method = method, lot = len(result))

        return result






class dwnX( uty, lv ):

    global STORE

    def __init__(self, name, Id = None, level = 'debug', asy = False, **kwargs):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)


        self.name = name
        self.asy = asy
        self.kwargs = kwargs
        self.level = level

    def __enter__(self):

        pre = STORE.get(self.name)

        if pre == None:

            if self.asy:

                STORE[self.name] = dwnXasync(Id = self.Id, level = self.level, name = self.name, **self.kwargs)
                
            else:

                STORE[self.name] = dwnXsync(Id = self.Id, level = self.level, name = self.name, **self.kwargs)

                STORE[self.name].__enter__()

                

        return STORE[self.name]


 

    def __exit__(self, *args, **kwargs):

        pass



#add eCode for facilaiting response and a lambda function to execute if code not met      


##header = {
##        'Host':'www.nseindia.com', 
##        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
##        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 
##        'Accept-Language':'en-US,en;q=0.5', 
##        'Accept-Encoding':'gzip, deflate, br',
##        'DNT':'1', 
##        'Connection':'keep-alive', 
##        'Upgrade-Insecure-Requests':'1',
##        'Pragma':'no-cache',
##        'Cache-Control':'no-cache',    
##    }
##
##    
##
##
##
##

#status eCode add



##with dwnX('ini', headers = header) as dwn:
##
##    url = 'https://www.nseindia.com/get-quotes/equity?symbol=TCS'
##
##    resp = dwn.pull(url = url)[url]
##
##    req_cookies = dict(nsit=resp.cookies['nsit'],
##                       nseappid=resp.cookies['nseappid'],
##                       ak_bmsc=resp.cookies['ak_bmsc'])
##
##
##
##
##with dwnX('quote', asy = True, headers = header, cookies = req_cookies) as dwn:
##
##    url = 'https://www.nseindia.com/api/quote-equity?symbol=TCS'
##
##    resp = dwn.pull([{'kwargs':{'url':url}}])[url]
##
##    print(resp.text)
##
##    
##

    

        


##with dwnX('quote', asy = True, headers = header) as dwn:
##
##    task = []
##
##    for each in range(9):
##
##        task.append({"kwargs":{"url":"https://www.nseindia.com/get-quotes/equity?symbol=TCS"}})
##
##    print(dwn.pull(task))

        

        

        
