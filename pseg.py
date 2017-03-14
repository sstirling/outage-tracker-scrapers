import urllib
from bs4 import BeautifulSoup
import time
from outagetools import FixedOffset
from datetime import datetime
import psycopg2
import sys


company = "PSE&G"
#connect to the database
conn = psycopg2.connect("dbname=nsl_outages user=nsl_out password=powerout")
cur = conn.cursor()
#create a list of counties serviced by PSEG
counties = ['Bergen','Burlington','Camden','Essex','Gloucester','Hudson','Hunterdon','Mercer','Middlesex','Monmouth','Morris','Passaic','Somerset','Union']
#assign a timestamp for when we accessed the data. This timestamp is passed into the python script from the bash script where it is created
currenttime = datetime.strptime(" ".join(sys.argv[1:]),'%a %b %d %H:%M:%S %Z %Y')
#open a file to write to - temporary until db connection is built
#writer = open("pseg.csv","wb")
#write in the header - temporary until db connection is built
#writer.write('company,updated,timestamp,county,muni,outages,totalcustomers\r')
#cycle through the counties
for county in counties:
    #construct the url for the scrape based on which county we are looking at
    url = 'http://www.pseg.com/outagemap/Customer%20Outage%20Application/Web%20Pages/GML/'+county+'.gml'
    #open the county page and read it into the variable
    file = urllib.urlopen(url)
    file = file.read()
    #load the variable into BeautifulSoup
    soup = BeautifulSoup(file)
    #find the timestamp of the file
    updatetime = soup.find('timestamp').text
    #find each of the towns within the county service area
    members = soup.findAll('gml:featuremember')
    #begin stepping through these towns to extract the data
    for member in members:
        #Start with find the municipality name
        muni = member.find('ms:muni').text
        #Then find the number of outages for that municipality
        outages = member.find('ms:outage').text
        row = [company,updatetime,currenttime,county,muni,outages]
        cur.execute("INSERT INTO tracker_pseg(company,updated,timestamp,county,muni,outage) VALUES(%s,%s,%s,%s,%s,%s)",(row[0],row[1],row[2],row[3],row[4],row[5]))
        conn.commit()
        #writer.write(','.join([company, updatetime, currenttime, county,muni,outages,customers])+'\r')
cur.close()
conn.close()
#writer.close()