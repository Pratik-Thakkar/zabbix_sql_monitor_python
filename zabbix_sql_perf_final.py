import getpass
import sys
import time
from pyzabbix import ZabbixAPI, ZabbixAPIException
from datetime import datetime
import pandas as pd
import numpy as np
import requests

username = "Provide username" 
pas = getpass.getpass ()

clientid = []              # Stores cliend_ids fetched from mysql
t_clientid = []            # Stores targeted client_ids based on the datacenter provided
clientName = []            # Stores Client_Name fetched from mysql
t_clientName = []          # Stores targeted client_names based on the datacenter provided
hostname = []              # Stores hostnames fetched from zabbix
hostid = []                # Stores hostids for each hostname fetched from zabbix and will be used to get histrory
datacenter = []            # Stores datacenter information of the targeted clients from mysql
pcounter = []              # Stores names of the performance counters which are enabled in mysql
palias = []                # Stores alias of the enabled performance counter from mysql

# giving time frame for fetching data
time_till = time.mktime(datetime.utcnow().timetuple())
time_from = time_till - 60 * 60 * 1  # 1 hour

# all region datacenter information
regoins = ['CH3','LO5','AZCA','AZBR','AZDE','AZSG','AZAU']
regions_ip=['10.103.6.15','10.255.109.5','10.255.113.6','10.255.111.5','10.255.117.5','10.255.115.5','10.255.109.5']
zabi_url = zip(regoins,regions_ip)

# creating a function which connects to zabbix of specified datacenter. This function returns connection object
def connectZabbix(region):
    zapi = ZabbixAPI("http://"+region[1], user= username, password = pas)
    print("Connected to %s Zabbix API Version %s" % (region[0],zapi.api_version()))
    return zapi

# this function will convert seconds to hours.
def GetTime(seconds):
    return float(seconds/3600)

# this function will return average.
def avg(l):
    return sum(l)/int (len(l))

# creating a function which fetch host information.This function returns hostname, hostid, targeted client name and id.
def GetHostInfo(region):
    try:
        # fetching vm information which has type SQL, storing the client id and hostname
        clients = requests.get("https://<MYSQL IP>/opsconfig/api.php/vm_information?filter[]=vm_type,eq,SQL&transform=1&columns=host_name,client_id")
        if clients.status_code != 200:
            raise Exception("Got error code %s while fetching the performance counter" % clients.status_code)
        clients.content.decode("utf-8")
        clients = clients.json()

        for cl in clients["vm_information"]:
            clientName.append(cl["host_name"])  #push client name of the eligible host
            clientid.append(cl["client_id"])    #push client id of eligible host
        
        # fetching datacenter informaion for all the clients with type SQL and storing datacenter information
        for ci in clientid:
            dc = requests.get("https://<MYSQL IP>/opsconfig/api.php/customer_metadata?filter[]=client_id,eq,"+ci+"&transform=1&columns=hosted_datacenter")
            if dc.status_code != 200:
                raise Exception("Got error code %s while fetching the performance counter" % dc.status_code)
            dc.content.decode("utf-8")
            dc = dc.json()
            for d in dc["customer_metadata"]:
                datacenter.append(d["hosted_datacenter"])  #push datacenter information for each host

        hostInfo = zip(clientName,clientid,datacenter)  #This will store host information from mysql

        # storing host information
        for hi in hostInfo:
            if region in hi:
                cname = ["*" + hi[0] + "*"]
                for p in cname:
                    try:
                        hosts = zapi.host.get(output="extend", search={"host": p},searchWildcardsEnabled =1)
                        if not hosts:
                            raise Exception("Host %s is not available in %s zabbix" % (p, region))
                        for h in hosts:
                            hostname.append(h["name"])  #push target hostname
                            hostid.append(h["hostid"])  #push targer hostid
                    except Exception as e:
                        print(e)
                t_clientName.append(hi[0])  #push target client name
                t_clientid.append(hi[1])    #push targer clinet id
    except Exception as e:
        print(e)
        sys.exit(1)
    
    return (hostid,hostname,t_clientid)

# creating a function which will fetch all the enabled perfcounters.This function returns name of the counter and alias.
def GetPerfCounter():
    try:
        # fetching all enabled performance counters from mysql
        counters = requests.get("https://<MYSQL IP>/opsconfig/api.php/sql_perf_stat?transform=1&columns=perf_counter,alias,enabled")
        if counters.status_code != 200:
                raise Exception("Got error code %s while fetching the performance counter" % counters.status_code)
        counters.content.decode("utf-8")
        counters = counters.json()
        
        for ct in counters["sql_perf_stat"]:
            if(ct["enabled"] == 1):
                pcounter.append(ct["perf_counter"])   #push the performance counter name
                palias.append(ct["alias"])            #push the alias name
    except Exxception as e:
        print(e)
        sys.exit(1)
   
    return (pcounter,palias)

# creating a function which will be called with a performance counter and alias. This function returns datafame.
def GetData(key,alias):
    try:
        # calling function to get host information like hostid, hostname and clientid
        hostInfo = GetHostInfo(dc_name)
        if not hostInfo:
            raise Exception("Unable to find any sql host in %s dc_name" % dc_name)
    except:
        print("Error! The function GetHostInfo was not able to finish its execution for %s" %key)

    hostData = zip(t_clientid,hostname,hostid) # Data that will be pushed to dataframe

    for hd in hostData:
        itemid = []
        tvalues = []
        
        try:
            # This will hold item id for every performance counter
            values = zapi.item.get(hostids=hd[2],selectTriggers="triggerid",search={"key_":key})
            if not values:
                raise Exception("Unable to find itemid for host %s and perf_counter %s " % (hd[1],key))
            for i in values:
                itemid.append(i["itemid"])

            # fetching history values for the given performance counter
            hist = zapi.history.get(history=0,hostids=hd[2],itemids=itemid,time_from=time_from,time_till=time_till)
            if not hist:
                raise Exception("Unable to find history for host %s and itemid %s " % (hd[1],item))

            # createing list with all the values
            for item in itemid:
                trow = []
                for i in hist:
                    if (i["itemid"] == item):
                        trow.append(float(i["value"]))
                tvalues.append(trow)

            # creating panda dataframe
            cols = ("client_id","hname","hid","itemid","alias","values","min","max","avg","date_time")
            df = pd.DataFrame(columns=cols)

            # pushing values to dataframe
            df["client_id"] = pd.Series(hd[0])
            df["hname"] = pd.Series(hd[1])
            df["hid"] = pd.Series(hd[2])
            df["itemid"] = itemid
            df["alias"] = alias
            df["values"] = tvalues
            df["date_time"] = str(datetime.fromtimestamp(time_till).strftime('%Y-%m-%d %X'))
            
            # finding min, max and average. performing type conversion
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
                df["min"] = np.round(GetTime(df["min"]), decimals=2)
                df["max"] = np.round(GetTime(df["max"]), decimals=2)
                df["avg"] = np.round(GetTime(df["avg"]), decimals=2)
        except Exception as e:
            print(e)
            sys.exit(1)

    # calling function to push data
    try:
        sendData = PushData(df)
        if sendData.status_code != 200:
            raise Exception("Got error code %s while pushing data to SQL" % sendData.status_code)
    except:
        print("Error! The function PushData was not able to finish its execution for %s" %key)

    return df

# creating a function which will push data to mysql. This function returns status of the push.
def PushData(fd):
    # eleminating not required columns
    fd=fd.drop(columns=['hid', 'itemid', 'values'])

    # exporing data to mysql
    try:
        status = requests.post("https://<MYSQL IP>/opsconfig/api.php/sql_perf_data_tmp", data = fd.to_json(orient='records'))
        if status.status_code == 200:
            print("Data pushed successfully for performance counter %s" % str(fd["alias"]))
    except Exception as e:
        print(e)
        sys.exit(1)
        
    return(status)

# We will start calling each funtion and push the data to SQL database
#dc_name = input("Please enter the datacenter code: ")
dc_name = str(sys.argv[1])

# calling function for zabbix connection
for zabi in zabi_url:
    if dc_name in zabi:
        try:
            zapi = connectZabbix(list(zabi))
        except:
            print("Could not connect to %s zabbix" %dc_name)

# calling function to get performance counters
try:
    stats = GetPerfCounter()
    if not stats:
        raise Exception("No performance counter is enabled. Please enable performance counter in the perf_stat table.")
except:
    print("Error! The function GetPerfCounter was not able to finish its execution")

# calling function to get data and push to database
for i,j in zip(pcounter,palias):
    try:
        data = GetData(i,j)
    except:
        print("Error! The function GetData was not able to finish its execution for %s" %i)