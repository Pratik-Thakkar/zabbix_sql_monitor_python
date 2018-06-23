import getpass
import sys
import time
from pyzabbix import ZabbixAPI
from datetime import datetime
import pandas as pd
import sqlalchemy
import requests

username = "" 
pas = getpass.getpass ()

# calling zabbix api for connection
zapi = ZabbixAPI("http://10.103.6.15", user= username, password = pas)
print("Connected to Zabbix API Version %s" % zapi.api_version())

# giving time frame for fetching data
time_till = time.mktime(datetime.now().timetuple())
time_from = time_till - 60 * 60 * 1  # 1 hours

hostname = []
hostid = []

# get all hosts from the SQL servers group
hosts = zapi.hostgroup.get(groupids='16',selectHosts="extend") 

# creating list for hostname and hostid
for h in hosts:
    data = h["hosts"]
    for i in data:
        hostname.append(i["name"])
        hostid.append(i["hostid"])

# creating a function which will be called everytime with a performance counter
def perfcounter(key,filename):
    
    # This will hold item id for every performance counter
    itemid = []
    values = zapi.item.get(hostids=hostid,selectTriggers="triggerid",search={"key_":key})

    # creating list for item ids 
    for i in values:
        itemid.append(i["itemid"])

    # fetching history values for the given performance counter and saving in list
    tvalues = []
    count = len(itemid)
    hist = zapi.history.get(history=0,hostids=hostid,itemids=itemid,time_from=time_from,time_till=time_till)

    # createing list with all the values
    for item in itemid:
        trow = []
        for i in hist:
            if (i["itemid"] == item):
                trow.append(float(i["value"]))
        tvalues.append(trow)

    # creating panda dataframe
    cols = ("hname","hid","itemid","values")
    df = pd.DataFrame(columns=cols)

    # pushing values to dataframe
    df["hname"] = hostname
    df["hid"] = hostid
    df.drop(df.index[1], inplace=True) # we need to remove this once we solve Customer Support SQL DB 01
    df["itemid"] = itemid
    df["values"] = tvalues
    #df = df.sort_values("hname")

    def avg(l):
       return sum(l)/float (len(l))

    # finding min, max and average
    df["min"] = df.apply(lambda x: min(x["values"]), axis =1)
    df["max"] = df.apply(lambda x: max(x["values"]), axis =1)
    df["avg"] = df.apply(lambda x: avg(x["values"]), axis =1)
    
    # eleminating not required columns
    df=df.drop(columns=['hid', 'itemid', 'values'])
    
    # exporing data to mysql
    
    #engine = sqlalchemy.create_engine('mysql+pymysql://USER:PASSWORD@<MYSQL IP>:3306/sql_perf_monthly')
    #df.to_json(filename +".json")
    #print(df.to_json(orient='records'))
    
    requests.post("https://<MYSQL IP>/opsconfig/api.php/sql_perf_data", data = df.to_json(orient='records'))


# calling function perfcounter with required keys
print("Running Free disk space on D: (percentage)")
perfcounter("vfs.fs.size[D:,pfree]","/Users/pratikthakkar/Desktop/D_free_disk_space")
print("Running Free disk space on C: (percentage)")
#perfcounter("vfs.fs.size[C:,pfree]","/Users/pratikthakkar/Desktop/C_free_disk_space")
#perfcounter('perf_counter[\TCPv4\Connections Established,]',"/Users/pratikthakkar/Desktop/connections")