from uty import uty
from logVector import logVector as lv


import asyncio, aiohttp
from aiohttp import client_exceptions
from urllib3 import util



class mDwn( uty, lv ):

    def __init__(self, Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)


        

        

        


    def mRegular(self, urlList, ind = False):

        assoc = "mDwn:mRegular"

        ts = self.ts()

        self.retObj = {}

        
        reqList = []

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", urlList = urlList)

        
        for urlSeg in urlList:

            data = {'url':None,
                'enc':'rek',
                'method':'GET',
                'raw':False,
                'eCode':[200],
                'headers': {'Accept': '*/*',
                            'Accept-Language': 'en-US,en;q=0.5',
                            'Host': None,
                            'Referer': None,
                            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
                                           'X-Requested-With': 'XMLHttpRequest'
                            },
                'payLoad':None
                }

            host = util.parse_url(urlSeg['url']).host

            data['headers']['Host'], data['headers']['Referer']= host, host

            data.update(urlSeg)

            reqList.append(data)


        asyncio.run(self.__PULL__(reqList, ind))

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", lot = len(self.retObj))


        return self.retObj

        


    async def __fetch__(self, session, urlObj):

        ts = self.ts()

        assoc = 'mDwn:__fetch__'

        try:

            self.mTrace(stamp = ts, assoc = assoc, status = "GET", params = urlObj)

            resp = await session.get(urlObj['url'], headers = urlObj['headers'])

            

            if resp.status in urlObj['eCode']:


                try:
                
                    if urlObj['raw']:

                        self.retObj[urlObj['url']] = await resp.read()
                
                    elif urlObj['enc'] == 'rek':

                        self.retObj[urlObj['url']] = await resp.text()

                    else:

                        _ = await resp.read()
                        self.retObj[urlObj['url']] = _.decode(urlObj['enc'])



                    self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", url = urlObj['url'])


                


                except:

                    self.retObj[urlObj['url']] = await resp.read()



            else:

                print(resp.status, resp.url)

                self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'status code not matched')



        except aiohttp.client_exceptions.ClientConnectorError:

            self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'url not accessible')


        


    async def __PULL__(self, urlList, ind):

        async with aiohttp.ClientSession() as session:

            if ind:

                for urlObj in urlList:

                    await self.__fetch__(session, urlObj)

                    await asyncio.sleep(.1)


            else:

                tasks = [self.__fetch__(session, urlObj) for urlObj in urlList]

                await asyncio.gather(*tasks)


                
                
##            
##
##        
##t = mDwn().mRegular([{'url':'https://www.niftyindices.com/indices/equity/broad-based-indices/NIFTY-Midcap-100', 'raw':True},
##                     {'url':'https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-midcap-50', 'enc':'utf-8'},
##                     {'url':'https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-midcap-150', 'enc':'utf-8'}])
##
##t = mDwn().mRegular([{'url':'https://www.niftyindices.com/indices/equity/broad-based-indices/NIFTY-Midcap-100', 'raw':True},
##                     {'url':'https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-midcap-50', 'enc':'utf-8'},
##                     {'url':'https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-midcap-150', 'enc':'utf-8'}])
##
