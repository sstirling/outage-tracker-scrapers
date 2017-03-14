import psycopg2
from datetime import datetime


#connect to the database
conn = psycopg2.connect("dbname=nsl_outages user=nsl_out password=powerout")
cur = conn.cursor()

cur.execute('TRUNCATE TABLE tracker_summary')
conn.commit()

cur.execute("SELECT timestamp FROM tracker_pseg WHERE timestamp >= (SELECT max(timestamp) - interval \'24 hours\' FROM tracker_pseg) GROUP BY timestamp ORDER BY timestamp DESC")
results = cur.fetchall()
for result in results:
    cur.execute("SELECT timestamp, sum(outage) FROM tracker_jcpl WHERE date_trunc('minutes',timestamp) = date_trunc('minutes',%s) GROUP BY timestamp",result)
    try:
        jcpl = cur.fetchone()[1]
    except:
        jcpl = ""
    cur.execute("SELECT timestamp, sum(outage) FROM tracker_pseg WHERE date_trunc('minutes',timestamp) = date_trunc('minutes',%s) GROUP BY timestamp",result)
    try:
        pseg = cur.fetchone()[1]
    except:
        pseg = ""
    cur.execute("SELECT timestamp, sum(outage) FROM tracker_ace WHERE date_trunc('minutes',timestamp) = date_trunc('minutes',%s) GROUP BY timestamp",result)
    try:
        ace = cur.fetchone()[1]
    except:
        ace = ""
    '''print '----'
    print jcpl
    print '----'
    print pseg 
    print '----'
    print ace
    print '----'''
    cur.execute('INSERT INTO tracker_summary(timestamp, pseg, jcpl, ace) VALUES (%s,%s,%s,%s)',(result,pseg,jcpl,ace))
    conn.commit()
cur.close()
conn.close()
