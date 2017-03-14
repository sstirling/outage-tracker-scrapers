import psycopg2
from datetime import tzinfo, timedelta, datetime

#formats the urlfrag variables to represent time of update
def updatetime(z):
    year = int(z[0:4])
    month = int(z[5:7])
    day = int(z[8:10])
    hour = int(z[11:13])
    min = int(z[14:16])
    newtime = '%d/%d/%d %d:%d' % (month,day,year,hour,min)
    return newtime
    

#class that converts UTC to EST, need to check if it supports daylight saving time
class FixedOffset(tzinfo):
    def __init__(self, offset):
        self.__offset = timedelta(hours=offset)
        self.__dst = timedelta(hours=offset-1)
        self.__name = ''
        
    def utcoffset(self, dt):
        return self.__offset
        
    def tzname(self, dt):
        return self.__name
    
    def dst(self, dt):
        return self.__dst
        