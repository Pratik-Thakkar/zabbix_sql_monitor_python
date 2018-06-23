import pymysql
import paramiko
import pandas as pd
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder
from os.path import expanduser

home = expanduser('~')
#mypkey = paramiko.RSAKey.from_private_key_file(home + pkeyfilepath)

sql_hostname = '127.0.0.1'
sql_username = ''
sql_password = ''
sql_main_database = 'db'
sql_port = 3306
ssh_host = 'ip_of_host'
ssh_user = ''
ssh_password = ''
ssh_port = 22
#sql_ip = '1.1.1.1.1'

with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        remote_bind_address=(sql_hostname, sql_port)) as tunnel:
    conn = pymysql.connect(host='127.0.0.1', user=sql_username,
            passwd=sql_password, db=sql_main_database,
            port=tunnel.local_bind_port)
    query = '''SELECT * from autogen_ref;'''
    data = pd.read_sql_query(query, conn)
    conn.close()