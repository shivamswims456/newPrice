import json, os
from uty import uty, JSON
from datetime import date

class logVector( object ):

    def __init__(self, dumpFile, logFile, Id, level = 'debug'):

        self.uty = uty()

        self.pathMaster = self.uty.pathMaster


        self.Id = Id
        self.dumpFile = dumpFile
        self.logFile = logFile
        self.level = level

        self.logForm = '{} -> {}\n'
                        #Id -> message

        self.message = '{}:{}:{}:{}:{}'
                        #time:messageReason:{module:function}:status:message

        self.actDate = date.today()

        self.actFile = open(os.path.join(self.pathMaster['dumps'], str(date.today()) + '.dump'), 'a+')

        
    def getActive(self):


        if self.actDate != date.today():

            self.actFile = open(os.path.join(self.pathMaster['dumps'], str(date.today()) + '.dump'), 'a+')

            self.actDate = date.today()

            print(self.actDate, self.actFile)

        return self.actFile

        
        


    def __wSeg__(self, message):

        file = self.getActive()
        file.write(self.logForm.format(self.Id, message))
        file.flush()

        


    def msgString(self, Dict):

        r = ''

        for k, v in Dict.items():

            r += '{} = {}, '.format(k, JSON.dumps(v))

        return r[:-2]


    def createLog(self, stamp = None):

        result = dict()

        

        if stamp == None:

            stamp = date.today()


        dumpFile = dumpFile = os.path.join(self.pathMaster['dumps'], str(stamp) + '.dump')
        logFile = os.path.join(self.pathMaster['logs'], str(stamp) + '.log')    
        

            


        with open(dumpFile) as f:

            text = f.read()


        text = text.split('\n')

        for line in text:

            if '->' in line:

                idSplit = line.split('->')

                result[idSplit[0]] = idSplit[1]

                

            

        with open(logFile, 'w') as f:

            json.dump(result, f, indent = 4)


        return logFile
        


    def mTrace(self, stamp, assoc, status, **message):

        reason = 'message'

        message = self.message.format(stamp, reason, assoc, status, self.msgString(message))

        self.__wSeg__(message)

        if self.level.lower() == 'debug':

            print(message)

        
        


    def eTrace(self, stamp, assoc, status, **message):

        reason = 'error'

        message = self.message.format(stamp, reason, assoc, status, self.msgString(message))

        self.__wSeg__(message)

        if self.level.lower() == 'debug':

            print(message)

            quit()

        

        
