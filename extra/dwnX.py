import httpx
from uty import JSON


STORE = {}



class dwnXobj( httpx.Client ):

    global STORE

    def __init__(self, name, *args, **kwargs):

        self.name = name

        self.state = None

        self.test = lambda: self.state.text

        httpx.Client.__init__(self, *args, **kwargs)
        
        self.__give__()

        self.route = {"GET":self.get,
                      "POST":self.post,
                      "PUT":self.put,
                      "DELETE":self.delete} #to be cont for more


    def __give__(self):

        return self

    def __qs__(self, **kwargs):

        print('i choise')

    def regular(self, method = "GET", **kwargs):

        method = self.route.get(method, self.__qs__)

        self.state = method(**kwargs)

        return self.state




    def getStore(self):

        return STORE

        

        




class dwnX( object ):

    global STORE

    def __init__(self, name = '', asy = False):

        self.name = name
        self.asy = asy
        

    def __enter__(self):

        pre = STORE.get(self.name)

        if pre == None:

            if self.asy:

                #pass async with 

              
            else:

                STORE[self.name] = dwnXobj(self.name)
                

        STORE[self.name].__enter__()
        
        return STORE[self.name]

    
    def __exit__(self, *args, **kwargs):

        STORE[self.name].__exit__()


        
    

header = {
        'Host':'www.nseindia.com', 
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8', 
        'Accept-Language':'en-US,en;q=0.5', 
        'Accept-Encoding':'gzip, deflate, br',
        'DNT':'1', 
        'Connection':'keep-alive', 
        'Upgrade-Insecure-Requests':'1',
        'Pragma':'no-cache',
        'Cache-Control':'no-cache',    
    }




with dwnX('quote', headers = header) as dwn:


    dwn.regular(url = 'https://www.nseindia.com/get-quotes/equity?symbol=TCS')

    for each in range(9):

        print(dwn.regular(url = 'https://www.nseindia.com/api/quote-equity?symbol=TCS').status_code)



    
        
