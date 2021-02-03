from datetime import datetime, date, timedelta

from uty import uty, JSON
from logVector import logVector as lv
from db import db




class baseQuery( uty, lv ):

    def __init__(self, Id = None, level = 'debug'):

        """
        any result returned as False is not supposed to be updated
        from any function down below
        """

        uty.__init__(self)

        self.Id = self.getId(Id)

        self.USER = 'baseQuery'

        self.db = db(Id = self.Id, level = level)

        self.finCache = {}



            
    def intervalStatus(self, tab, start, stop):

        result, reqSet = [], set()

        contDateCheck = lambda x : seg[-1] == x - timedelta(days = 1)

        with self.db as cu:

            q = f'select date(`intStamp`), count(`intStamp`) from `intData`.{tab} where `intStamp` between "{start}" and "{stop}" group by date(`intStamp`);'

            availDates = cu.execute(q, func = lambda _: set(each[0] for each in _ if each[1] == 375))[q]

            #holiday Status update to be made

            q = f'select `hStamp` from `hNse` where `hStamp` between "{start}" and "{stop}";'

            holidays  = cu.execute(q, func = lambda _: {each[0] for each in _})[q]


            availDates = availDates.union(holidays)


        

        while start <= stop:

            reqSet.add(start)

            start += timedelta(days = 1)


        leftOut = iter(sorted(list(reqSet - availDates)))

        _ = next(leftOut)

        seg = [_, _]

        for Date in leftOut:

            if contDateCheck(Date):

                seg[1] = Date

            else:

                result.append(seg)

                seg = [Date, Date]


        result.append(seg)


        return result if len(result) else False
 
        
    def NseFinStat(self, start, stop, hlSet = set(), upd = False):

        _funcStat_ = 'NseFinStat'

        with self.db as cu:

            if not len(self.finCache):

                #if finCache is not present

                q = f'select `nFunc` from `fStat` where `nFunc` = "{_funcStat_}"'

                avail = cu.execute(q)[q]

                if len(avail):

                    #if func with its parameter is present

                    temp = set()

                    q = f'select `pFunc` from `fStat` where `nFunc` = "{_funcStat_}"'

                    rng = cu.execute(q, func = lambda _: {k : datetime.strptime(v, '%Y-%m-%d').date() for k, v in JSON.loads(list(_)[0][0]).items()})[q]


                    cStart, cStop = rng['start'], rng['stop']

                    self.finCache.update({"start":cStart, "stop":cStop})

                    while cStart <= cStop:

                        temp.add(cStart)

                        cStart += timedelta(days = 1)

                    self.finCache["rng"] = temp


                else:

                    #if func is not present in database

                    self.finCache["rng"] = False

                    obj = JSON.dumps({"start":start, "stop":stop})

                    q = f"replace into `fStat` (`nFunc`, `pFunc`) values ('{_funcStat_}', '{obj}')"


                    cu.execute(q)

                    cu.cnx.commit()



            #resolving dates needed to download

            obj = {}
                        
            if self.finCache["rng"]:

                if start < self.finCache["start"]:

                    obj["start"], self.finCache["start"] = start, start

                

                if stop < self.finCache["stop"]:

                    obj["stop"], self.finCache["stop"] = stop, stop


                if not len(obj):

                    reqRange = False

                
                


        

        rng = set()

        while start <= stop:

            rng.add(start)

            start += timedelta(days = 1)



        result = rng - hlSet
        
        if self.finCache["rng"]:

            result -= self.finCache["rng"]



        result = sorted(list(result), reverse = True)

        
        
        if len(obj):

            self.finCache["rng"] = rng
            obj["start"] = obj.get("start", self.finCache["start"])
            obj["stop"] = obj.get("stop", self.finCache["stop"])

            obj = JSON.dumps(obj)

            with self.db as cu:

                q = f"replace into `fStat` (`nFunc`, `pFunc`) values ('{_funcStat_}', '{obj}')"

                cu.execute(q)

                cu.cnx.commit()


        return result

                    


    def holidayStatus(self, start, stop):

        result = {'start':start, 'stop':stop}

        with self.db as cu:

            q = 'select `hStamp` from `hNse`'

            _ = cu.execute(q)[q]


        
        if len(_):

            st = _[0].year

            sp = _[-1].year

            if start <= st and stop <= sp:

                result = False


        return result



            





#baseQuery().holidayStatus(start = 2020, stop = 2021)
#print(baseQuery().NseFinStatus(start = date.today().replace(day = 25), stop = date.today()))
print(baseQuery().intervalStatus(tab = 'TCS_1m', start = date.today().replace(day = 14, month = 1), stop = date.today()))
        

        

        
