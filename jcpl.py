import urllib
import time
import simplejson as json
from bs4 import BeautifulSoup
from outagetools import updatetime, FixedOffset
from datetime import datetime
import psycopg2
import sys



company = 'JCP&L'
#connect to the database
conn = psycopg2.connect("dbname=nsl_outages user=nsl_out password=powerout")
cur = conn.cursor()
#set the timestamp that will be used in our next two scrapes
timestamp = time.time()
#assign a timestamp for when we accessed the data. This timestamp is passed into the python script from the bash script where it is created
timestamp2 = datetime.strptime(" ".join(sys.argv[1:]),'%a %b %d %H:%M:%S %Z %Y')
#writer = open('jcpl.csv','wb')
#writer.write('company,updated,timestamp,county,muni,outage,totalcustomers\r')


#scrape the directory where the data is stored
url1 = 'http://outages.firstenergycorp.com/data/interval_generation_data/metadataNJ.xml?timestamp=%d' % timestamp
urlpart = urllib.urlopen(url1)
urlpart = urlpart.read()
soup = BeautifulSoup(urlpart)
#extract the directory from the xml structure
urlfrag = soup.find('directory').text
#get the update time from the urlfrag variable
update = updatetime(urlfrag)

#insert the directory into the url for  the next scrape
url = 'http://outages.firstenergycorp.com/data/interval_generation_data/'+urlfrag+'/report.js?timestamp=%d' % timestamp
file = urllib.urlopen(url)
file = file.read()
file = json.loads(file)

#begin parsing the json by stepping through to the county level of the data
counties = file['file_data']['curr_custs_aff']['areas'][0]['areas']
#loop through the counties to retrieve municipal level data
for county in counties:
    countyname = county['area_name'].title()
    munis = county['areas']
    #loop through the municipalities to retrieve their data
    for muni in munis:
        municipality = muni['area_name'].title()
        customers = muni['total_custs']
        outages = muni['custs_out']
        row = [company,update,timestamp2,countyname,municipality,str(outages),str(customers)]
        cur.execute("INSERT INTO tracker_jcpl(company,updated,timestamp,county,muni,outage,customers) VALUES(%s,%s,%s,%s,%s,%s,%s)",(row[0],row[1],row[2],row[3],row[4],row[5],row[6]))
        conn.commit()
        #writer.write(company+','+update+','+timestamp2+','+countyname+','+municipality+','+str(outages)+','+str(customers)+'\r')
cur.close()
conn.close()
#writer.close()
