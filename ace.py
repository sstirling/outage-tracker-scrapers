import urllib
import time
import simplejson as json
from bs4 import BeautifulSoup
from outagetools import updatetime, FixedOffset
from datetime import datetime
import psycopg2
import sys


company = 'ACE'
#connect to the database
conn = psycopg2.connect("dbname=nsl_outages user=nsl_out password=powerout")
cur = conn.cursor()
#set the timestamp that will be used in our next two scrapes
timestamp = time.time()
#assign a timestamp for when we accessed the data. This timestamp is passed into the python script from the bash script where it is created
timestamp2 = datetime.strptime(" ".join(sys.argv[1:]),'%a %b %d %H:%M:%S %Z %Y')
#writer = open('ace.csv','wb')
#writer.write('company,updated,timestamp,county,muni,outage,totalcustomers\r')

#scrape the directory where the data is stored
url1 ='http://stormcenter.atlanticcityelectric.com.s3.amazonaws.com/data/interval_generation_data/metadata.xml?timestamp=%d' % timestamp
urlpart = urllib.urlopen(url1)
urlpart = urlpart.read()
soup = BeautifulSoup(urlpart)
#extract the directory from the xml structure
urlfrag = soup.find('directory').text
#get the update time from the urlfrag variable
update = updatetime(urlfrag)

#insert the directory into the url for  the next scrape
url ='http://stormcenter.atlanticcityelectric.com.s3.amazonaws.com/data/interval_generation_data/'+urlfrag+'/report.js?timestamp=%d' % timestamp
file = urllib.urlopen(url)
file = file.read()
file = json.loads(file)

#begin parsing the data, need to figure out if ACE reports their data by the county level only
areas = file['file_data']['curr_custs_aff']['areas']
#ACE reports their entire coverage area as one level in the json file
for area in areas:
    counties = area['areas']
    for county in counties:
        countyname = county['area_name']
        customers = county['total_custs']
        outages = county['custs_out']
        row = [company,update,timestamp2,countyname,str(outages),str(customers)]
        cur.execute("INSERT INTO tracker_ace(company,updated,timestamp,county,outage,customers) VALUES(%s,%s,%s,%s,%s,%s)",(row[0],row[1],row[2],row[3],row[4],row[5]))
        conn.commit()

cur.close()
conn.close()
        #writer.write(company+','+update+','+timestamp2+','+countyname+','+muni+','+str(outages)+','+str(customers)+'\r')
#writer.close()