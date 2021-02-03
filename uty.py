import os, csv, re, json, decimal
from json import JSONEncoder
from datetime import datetime, date, timedelta
from uuid import uuid4
from bs4 import BeautifulSoup as bsp
from pprint import pprint


class JSON( object ):

    def dumps(obj, *args, **kwargs):

        return json.dumps(obj, *args, **kwargs, cls = jEncoder)

    def dump(obj, *args, **kwargs):


        return json.dump(obj, *args, **kwargs, cls = jEncoder)


    def loads(obj, *args, **kwargs):

        return json.loads(obj, *args, **kwargs)


    def load(obj, *args, **kwargs):

        result = False

        with open(obj) as f:

            result = json.load(f)


        return result

            




class uty( object ):

    def __init__(self):

        self.path = os.getcwd()

        self.enc = 'utf-8'

        self.dec = decimal.Decimal

        self.pathMaster = {'dbStruct':os.path.join(self.path, 'configs', 'dbStruct.json'),
                           'dbConfigs':os.path.join(self.path, 'configs', 'dbConfigs.json'),
                           'exchanges':os.path.join(self.path, 'configs', 'exchanges.json'),
                           'brokers_':lambda bnm:os.path.join(self.path, 'configs', bnm),
                           'env':os.path.join(self.path, 'app.env'),
                           'dumps':os.path.join(self.path, 'dumps'),
                           'logs':os.path.join(self.path, 'logs')}


        self.ts = datetime.now

        self.td = lambda ts : (datetime.now() - ts).total_seconds()

        self.getId = lambda Id : str(uuid4()) if Id == None else Id

        self.dbId = lambda: str(uuid4()).replace('-', '')

        self.bsp = bsp

        self.exchangeBook = None

        self.__loadExgs__()

        self.print = pprint

        self.re = re
        

    
        
    def stamps(self, start, stop, result = set()):

        if isinstance(result, set):

            while start <= stop:

                result.add(start)

                start += timedelta(days = 1)

        return result
        
            

        

    def splString(self, string, sub = '_'):

        if string != None:

            result = re.sub('[^A-Za-z0-9]+', sub, string)


        else:

            result = string


        return result


    def bTj(self, elem, pos = []):

        result = []

        for each in elem:

            result.append(list(each))

            for _ in pos:

                result[-1][_] = JSON.loads(result[-1][_])


                

        return result

                


    def qTo(self, cursor, fc, func = None):

        result = False

        if func != None:

            result = func(self.cursor)

        elif fc:

            result = [[_ for _ in each] for each in cursor]

        else:

            result = [each[0] for each in cursor]


        return result

        


        


    def csvBase(self, string, headers = [], ff = None, sub = 'null'):


        csvObj = list(csv.reader(string.splitlines()))

        fileHeads = [each.strip() for each in csvObj[0]]

        reqHeads = []

        result = []

        


        for head in headers:

            if head in fileHeads:

                reqHeads.append(fileHeads.index(head))

            else:

                reqHeads.append(None)



        

        


        for seg in csvObj[1:]:

            temp = []

            for pos in reqHeads:

                if pos != None:

                    temp.append(seg[pos].strip())


                else:

                    temp.append(sub)


                    


            if ff != None:

                r = ff(temp)

                if r != None:

                    result.append(r)

            else:

                result.append(tuple(temp))




        return result

    def yahooExg(self, exg):

        return self.exchangeBook[exg]['yExg']


    def exgTimes(self, exg, seg = 'EQ'):

        result = False

        exg = self.exchangeBook.get(exg)

        if exg != None:

            result = exg['timeings'][seg]

        return result


    def __loadExgs__(self, force = False):


        if force or self.exchangeBook == None:

            with open(self.pathMaster['exchanges']) as f:

                self.exchangeBook = json.load(f)


        for exg in self.exchangeBook:

            for seg in self.exchangeBook[exg]['timeings']:

                self.exchangeBook[exg]['timeings'][seg] = [datetime.strptime(self.exchangeBook[exg]['timeings'][seg][0], '%H:%M:%S').time(),
                                                           datetime.strptime(self.exchangeBook[exg]['timeings'][seg][1], '%H:%M:%S').time()]
                


        return self.exchangeBook






   


class jEncoder(JSONEncoder):

    def default(self, obj):

        if isinstance(obj, date):

            return obj.strftime('%Y-%m-%d')


        elif isinstance(obj, datetime):

            return obj.strftime('%Y-%m-%d %H:%M:%S')


        elif isinstance(obj, decimal.Decimal):

            return str(obj)


        return super(extraEncoder, self).default(obj)



#print(JSON.loads('{"1":"1"}'))
#print(uty().exchangeBook)
