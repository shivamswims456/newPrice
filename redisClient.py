from uty import uty, JSON
from logVector import logVector as lv


import redis
from subprocess import check_output


class redisClient( uty, lv ):

    def __init__(self, db = None, flush = False, Id = None, level = 'debug'):

        uty.__init__(self)

        self.Id = self.getId(Id)

        lv.__init__(self, dumpFile = self.pathMaster['dumps'],
                    logFile = self.pathMaster['logs'],
                    Id = self.Id,
                    level = level)

        self.auth = JSON.load(self.pathMaster['dbConfigs'])['redis']

        self.redisStrcut = JSON.load(self.pathMaster['dbStruct'])['redis']

        self.r = redis.Redis(host = self.auth['host'],
                             port = self.auth['port'],
                             password = self.auth['passWord'],
                             db = self.auth['db'] if db == None else self.redisStrcut[db])

        if flush:

            self.r.flushdb()

        

        self.__setUp__()


    def __setUp__(self):

        assoc = "redisClient:__setUp__"

        ts = self.ts()

        out = check_output(["systemctl", "is-active", "redis"]).decode('utf-8')

        status = lambda:False if out.strip().lower() == "active" else True

        stat = status()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", check = stat)

        if stat:

           check_output(["systemctl", "start", "redis"])

        self.mTrace(stamp = self.td(ts), assoc = assoc, status = "SUCCESS", check = stat)



    def rSet(self, Dict):

        ts = self.ts()

        assoc = "redicClient:rSet"

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", toSet = len(Dict))

        Dict = {k:str(v) for k, v in Dict.items()}

        try:

            result = self.r.mset(Dict)

        except redis.exceptions.AuthenticationError:

            self.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", reason = "Can't connect to redis")


        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", toSet = result)

        return result

    


    def rGet(self, key):

        assoc = "redisClient:rGet"

        ts = self.ts()

        self.mTrace(stamp = ts, assoc = assoc, status = "GET", key = key)

        value = self.r.get(key)

        if value != None:

            try:

                value = eval(self.r.get(key).decode('utf-8'))

            except redis.exceptions.AuthenticationError:

                self.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", reason = "Can't connect to redis")


        else:

            self.eTrace(stamp = self.ts(), assoc = assoc, status = "ERROR", reason = "KEY NOT FOUND")

        self.mTrace(stamp = ts, assoc = assoc, status = "SUCCESS", lot = len(value))

        return value



#redisClient()

#print(redisClient().rSet({"symbolList":["care"]}))
#print(redisClient().rGet("TCS"))
