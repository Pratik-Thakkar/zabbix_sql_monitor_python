import getpass
import sys
import time
from pyzabbix import ZabbixAPI, ZabbixAPIException
from datetime import datetime
import pandas as pd
import numpy as np
import requests

username = "" 
pas = getpass.getpass ()

regoins = ['CH3','LO5','AZCA','AZBR','AZDE','AZSG','AZAU']
regions_ip=['10.103.6.15','10.255.109.5','10.255.113.6','10.255.111.5','10.255.117.5','10.255.115.5','10.255.109.5']
zabi_url = zip(regoins,regions_ip)

dc_name = input("Please enter the datacenter code: ")

for i in zabi_url:
    if dc_name in i:
        try:
            zapi = connectZabbix(list(i))
        except:
            print("Could not connect to %s zabbix" %dc)

# calling zabbix api for connection
def connectZabbix(region):
    zapi = ZabbixAPI("http://"+region[1], user= username, password = pas)
    print("Connected to %s Zabbix API Version %s" % (region[0],zapi.api_version()))
    return zapi
    
# giving time frame for fetching data
time_till = time.mktime(datetime.utcnow().timetuple())
time_from = time_till - 60 * 60 * 1  # 1 hour
print(datetime.fromtimestamp(time_till).strftime('time_till :%c'))
print(datetime.fromtimestamp(time_from).strftime('time_from :%c'))

clientid = []
client_name = []
hostname = []
hostid = []
hosts = []
dataCenter = []

clients = requests.get("https://<MYSQL IP>/opsconfig/api.php/vm_information?filter[]=vm_type,eq,SQL&transform=1&columns=host_name,client_id")
clients.content.decode("utf-8")
clients = clients.json()

for cl in clients["vm_information"]:
    client_name.append(cl["host_name"])
    clientid.append(cl["client_id"])
    
for ci in clientid:
    dc = requests.get("https://<MYSQL IP>/opsconfig/api.php/customer_metadata?filter[]=client_id,eq,"+ci+"&transform=1&columns=hosted_datacenter")
    dc.content.decode("utf-8")
    dc = dc.json()
    for d in dc["customer_metadata"]:
        dataCenter.append(d["hosted_datacenter"])

target = zip(client_name,clientid,dataCenter)

# get all hosts from the SQL servers group
for di in target:
    print(di)
    if dc_name in di:
        client_name = ["*" + di[0] + "*"]
        print(client_name)
        for p in client_name:
            hosts.append(zapi.host.get(output="extend", search={"host": p},searchWildcardsEnabled =1))

# creating list for hostname and hostid
for h in hosts:
    hostname.append(h[0]["name"])
    hostid.append(h[0]["hostid"])
        
print(hostid)
print(hostname)
print(clientid)

pcounter = []
palias = []

counters = requests.get("https://<MYSQL IP>/opsconfig/api.php/sql_perf_stat?transform=1&columns=perf_counter,alias,enabled")
counters.content.decode("utf-8")
counters = counters.json()

for ct in counters["sql_perf_stat"]:
    if(ct["enabled"] == 1):
        pcounter.append(ct["perf_counter"])
        palias.append(ct["alias"])
print(pcounter)
print(palias)

def GetTime(seconds):
 for s in seconds:
    time = float(s)
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    return ("%dd %dh %dm" % (day, hour, minutes))

# creating a function which will be called everytime with a performance counter
def perfcounter(key,alias):
    
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
    #df.drop(df.index[1], inplace=True) # we need to remove this once we solve Customer Support SQL DB 01
    df["itemid"] = itemid
    df["values"] = tvalues
    #df = df.sort_values("hname")

    def avg(l):
       return sum(l)/int (len(l))

    # finding min, max and average
    df["min"] = df.apply(lambda x: min(x["values"]), axis =1)
    df["max"] = df.apply(lambda x: max(x["values"]), axis =1)
    df["avg"] = df.apply(lambda x: avg(x["values"]), axis =1)
    
    df["min"] = np.round(df["min"], decimals=2)
    df["max"] = np.round(df["max"], decimals=2)
    df["avg"] = np.round(df["avg"], decimals=2)
    
    if(alias == "st113" or alias == "str120" or alias == "str123" or alias == "str125" or alias == "str132"):
        df["min"] = np.ceil(df["min"])
        df["max"] = np.ceil(df["max"])
        df["avg"] = np.ceil(df["avg"])
        
    if(alias == "st115"):
        df["min"] = df.apply(lambda x: GetTime(x["values"]), axis =1)
        df["max"] = df.apply(lambda x: GetTime(x["values"]), axis =1)
        df["avg"] = df.apply(lambda x: GetTime(x["values"]), axis =1)
    
    # eleminating not required columns
    df=df.drop(columns=['hid', 'itemid', 'values'])
    df["alias"] = alias
    df["date_time"] = str(datetime.utcnow())[:19]
    df["client_id"] = clientid
    
    # exporing data to mysql
    requests.post("https://<MYSQL IP>/opsconfig/api.php/sql_perf_data", data = df.to_json(orient='records'))
    return df.to_json(orient='records')


# calling function perfcounter with required keys

for i,j in zip(pcounter,palias):
    print(perfcounter(i,j))