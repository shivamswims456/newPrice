"""
    module for downloading data from web singleThreaded
    
"""

from uty import uty
from logVector import logVector as lv

import urllib3, certifi, os





class dwn( uty, lv):

    def __init__(self, Id =  None, level = 'debug'):


        uty.__init__(self)
        self.Id = self.getId(Id)
        

        self.https = urllib3.PoolManager(
                cert_reqs = 'CERT_REQUIRED',
                ca_certs = certifi.where()
            )

        
        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)



        

        


    def regular(self, **kwargs):

        assoc = "dwn:regular"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = 'GET', params = kwargs)

        resp = False

        host = urllib3.util.parse_url(kwargs['url']).host

        data = {'url':None,
                'enc':'rek',
                'method':'GET',
                'raw':False,
                'eCode':[200],
                'headers': {'Accept': '*/*',
                            'Accept-Language': 'en-US,en;q=0.5',
                            'Host': host,
                            'Referer': host,
                            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
                                           'X-Requested-With': 'XMLHttpRequest'
                            }
                }


        data.update(kwargs)

        
        self.mTrace(stamp = ts, assoc = assoc, status = 'PUT', params = data)


        
        

        try:


            if 'fields' in data:

                resp = self.https.request(data['method'],
                                         data['url'],
                                         headers = data['headers'],
                                         fields = data['fields'])

            elif 'data' in data:

                resp = self.https.request(data['method'],
                                         data['url'],
                                         headers = data['headers'],
                                         body = data['data'])


            else:

                resp = self.https.request(data['method'],
                                         data['url'],
                                         headers = data['headers'])


        


        except urllib3.exceptions.MaxRetryError:

            self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'url not accessible')

        
    
        
        if resp:

            resp.release_conn()

            code = resp.status

            
            if code in data['eCode']:
                

                if data['raw']:

                    resp = resp.data


                elif data['enc'] == 'rek':


                    resp = ''.join(map(chr, resp.data))

                else:

                    resp = resp.data.decode(data['enc'])


            else:

                resp = False

                self.eTrace(stamp = self.ts(), assoc = assoc, status = 'ERROR', connection = 'status code not matched')


        self.mTrace(stamp = self.td(ts), assoc = assoc, status = 'SUCCESS', params = kwargs)

        

        return {kwargs['url']:resp}

            


