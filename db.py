import mysql.connector, asyncio, aiomysql, pymysql.err
from logVector import logVector as lv
from uty import uty, JSON

import warnings

warnings.filterwarnings('ignore', module=r"aiomysql")


class dbAsyn( uty, lv ):

    """
    asyncio version for executing async queries
    """

    def __init__(self, creds, Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)

        

        self.Id = self.getId(Id)

        self.creds = creds

        self.poolStatus =  False
        
        self.result = {}


        

    def tranSact(self, queryList):

        """
        cover function for async queries
        queryList = [{'query':''}]
        """

        assoc = "dbAsync:tranSact"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", queryList = queryList)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        loop.run_until_complete(self.__fetch__(queryList, loop))

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", lot = len(self.result))

        return self.result



    async def __fetch__(self, queryList, loop):

        #establishing fetchPool for requests


        assoc = "dbAsyn:__fetch__"

        try:
        
            pool = await aiomysql.create_pool(host = self.creds['host'],
                                               port = self.creds['port'],
                                               user = self.creds['userName'],
                                               password = self.creds['passWord'])


            self.mTrace(stamp = self.ts(), assoc = assoc, status = "INFO", reason = "connected Successfully")



        except pymysql.err.OperationalError:

            self.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", reason = "Can't connect to database")


        

        return await asyncio.gather(self.__tranSact__(loop = loop, pool = pool, queryList = queryList))

        


    async def __tranSact__(self, loop, pool, queryList):

        #actual taransact function for transacting requests


        async with pool.acquire() as conn:

            async with conn.cursor() as cur:

                for query in queryList:

                    query['query'] = query['query'].replace(',)', ')') # avoid single tuple Trap

                    Q = query['query']

                    if query.get('parse', True):

                        Q = self.parse(query['query'])


                    await cur.execute(Q)

                    result = await cur.fetchall()

                    await conn.commit()

                    self.result[query['query']] = self.qTo(result, fc = query.get('mode', False)) 

                    

                    

                

            

        

    

    
class dbSyn( uty, lv ):

    """

    sync version for executiong queries
    
    """

    def __init__(self, creds, Id = None, level = 'debug',):

        uty.__init__(self)

        self.Id = self.getId(Id)

        self.mysql = mysql.connector

        
        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)


    

        try:

            self.cnx = self.mysql.connect(user = creds['userName'],
                                          password = creds['passWord'],
                                          host = creds['host'],
                                          port = creds['port'])


            self.cursor = self.cnx.cursor()
            


        except mysql.connector.errors.ProgrammingError:


            self.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", reason = "Can't connect to database")

            

    
    def __get__():

        return self


    

    def tranSact(self, queryList):

        """
        {'query':select ..., 'mode':'True/False'}

        function for fetching a list of queries from database
        """

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", queryList = queryList)

        result = {}

        for query in queryList:

            query['query'] = query['query'].replace(',)', ')') # avoid single tuple Trap

            Q = query['query']

            if query.get('parse', True):

                Q = self.parse(query['query'])


            self.cursor.execute(Q)

            result[query['query']] = self.qTo(self.cursor, query.get('mode', False))


        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", lot = len(result))


        return result
            


    def execute(self, query, mode = False, parse = True, func = None):

        """
        single run query on database
        
        """

        assoc = "dbSyn:execute"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", mode = mode, parse = parse)

        query = query.replace(',)', ')') # avoid single tuple Trap

        Q = query

        if parse:

            Q = self.parse(query)

        

        self.cursor.execute(Q)

        result = self.qTo(self.cursor, mode, func)

        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", mode = mode, parse = parse, lot = len(result))

        return {query:result}


    

    

            

class db( uty, lv):

    """
    cover class for getting asyn or sync version of execution
    """


    def __init__(self, asy = False, Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        self.asy = asy

        self.level = level

        self.auth = JSON.load(self.pathMaster['dbConfigs'])['mySql']

        self.auth = {'userName':self.auth['userName'],
                     'passWord':self.auth['passWord'],
                     'host':self.auth['host'],
                     'port':self.auth['port']}


            
        self.varForm = '`{}`'
        

        self.schema = schema()
        self.nMap = self.schema.nMap
        self.makeSchema = self.schema.create



    def __qParse__(self, query):

        """
        function for parsing queryString with real names
        """

        
        for var in self.re.findall('`([^`]*)`', query):

            query = query.replace(self.varForm.format(var), self.nMap.get(var, 'null'))


        


        return query


        
    


    def __enter__(self):

        
        if self.asy:

            self.result = dbAsyn(creds = self.auth, Id = self.Id, level = self.level)

        else:

            self.result = dbSyn(creds = self.auth, Id = self.Id, level = self.level)

        
        self.result.parse = self.__qParse__
        


        return self.result



    def __exit__(self, *args, **kwargs):

        if self.asy:

            pass

        else:

            self.result.cnx.close()
            self.result.cursor.close()

            


class schema( uty, lv ):

    """
    class for creating databases from spec file
    """

    def __init__(self, Id = None, level = 'debug'):
        
        uty.__init__(self)
        self.Id = self.getId(Id)
        
        self.rawSchema = JSON.load(self.pathMaster['dbStruct'])['mySql']

        self.db = 'create database if not exists {};'
        self.tab = 'create table if not exists {}.{} {};'
        self.col = 'show columns from {}.{};'
        self.cCol = 'alter table {}.{} add column {} {};'
        self.aCol = 'alter table {}.{} modify {} {};'

        self.strip = lambda string: string.replace(' ', '').split('=>')[1]

        self.lStrip = lambda string: string.replace(' ', '').split('=>')

        self.mRoute = lambda string, node: '{}.{}'.format(string, node)

        self.nMap = {}

        self.__load__()




    def create(self):

        """
        create function
        
        """

        with db() as cu:

            for dtb, tabSeg in self.rawSchema.items():

                dtb = self.strip(dtb)

                cu.execute(query = self.db.format(dtb), parse = False)

                for tab, colSeg in tabSeg.items():

                    tab = self.strip(tab)

                    cu.execute(self.__cols__(dtb, tab, colSeg))

                    query = self.col.format(dtb, tab)

                    colList = cu.execute(query = query, mode = True, parse = False)[query]

                    colDict = {colInfo[0]:colInfo[1].lower().split(' ')[0] for colInfo in colList}

                    for col, tp in colSeg.items():

                        col = self.strip(col)

                        tp = tp.split(' ')[0].lower()

                        compCol = colDict.get(col)

                        if compCol == None:

                            cu.execute(query = self.cCol.format(dtb, tab, col, tp), parse = False)

                        elif compCol != tp:

                            cu.execute(query = self.aCol.format(dtb, tab, col, tp), parse = False)


    def __load__(self):

        """
        load map for queryName to dbName function
        """

        for dtb, tabSeg in self.rawSchema.items():

            _ = self.lStrip(dtb)

            dtb, self.nMap[_[0]] = _[1], _[1]

            for tab, colSeg in tabSeg.items():

                cmpTab = tab

                _ = self.lStrip(tab)

                
                tab = self.mRoute(dtb, _[1])

                self.nMap[_[0]] = tab

                for col, tp in colSeg.items():

                    _ = self.lStrip(col)

                    

                    if "__sample__" in cmpTab[1] :

                        self.nMap[_[0]] = self.mRoute(tab, _[1])

                    else:

                        self.nMap[_[0]] = _[1]
        


    
    def __cols__(self, db, tab, colSeg):

        q = '('

        for col, val in colSeg.items():

            col = self.strip(col)

            q += '{} {}, '.format(col, val)

        
        result = self.tab.format(db, tab, q[:-2] + ')')

        
        return result


                            

#schema(level = 'prod').create()



##
##with db() as cu:
##
##    print(cu.execute(query = 'show tables from `pg`;'))

    
##
##with db(asy = True) as cu:
##
##    print(cu.tranSact([{'query':'show databases', 'parse':False}]))

    
##
##    print(cu.tranSact([{'query':'show tables from priceGeneral', 'parse':False}]))
    
    
