#!/usr/bin/env python
# coding: utf-8

# Documentation of using SFTP server using python
# https://ourcodeworld.com/articles/read/813/how-to-access-a-sftp-server-using-pysftp-in-python
# 
# https://hdfs3.readthedocs.io/en/latest/index.html

# Documentation aboyt pysftp : 
# https://readthedocs.org/projects/pysftp/downloads/pdf/release_0.2.9/
# 
# 
# https://stackoverflow.com/questions/432385/sftp-in-python-platform-independent

# In[4]:


### Import all libraries

import pysftp
import pandas as pd
import json
import requests
from datetime import datetime

myHostname = "xx.xx.xx.xx"
myUsername = "username"
myPassword = "pass"
myport = 22


# In[17]:


#help( pysftp.Connection)


# In[6]:


cnopts = pysftp.CnOpts()

cnopts.hostkeys = None

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword,port=22, cnopts=cnopts) as sftp:
    print("Connection succesfully established ...Successfully able to go inside the SFTP server ")

    # Switch to a remote directory
    sftp.cwd('/export/home/mission')

    # Obtain structure of the remote directory '/var/www/vhosts'
    directory_structure = sftp.listdir_attr()

    # Print data
    for attr in directory_structure:
        print(attr.filename, attr)
        print("Successfully Printed directory structure")


# In[8]:


## Connect to impala and run the query

query = """
SELECT *
 FROM  wifi.table_name a 
"""



# In[9]:



query = query.replace('\n', ' ')

import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
 

 ## The below command is used to obtain environment variables
load_dotenv()
 
 
## Enter the following parameters -- Please replace your Username and password before running
 
## hostname of impala
hostname = 'host'
user ='user'
pw =os.environ.get('USER_PWD')
 
 
## Path of the certificate-- replace this path using the path of the certificate on your machine
cert ="C:\\Program Files\\Java\\jre1.8.0_201\\lib\\security\\root_cer.pem"
 
# Configuration settings for the ODBC connection
cfg = {'Driver': 'Cloudera ODBC Driver for Impala', 'host': hostname, 'port': 21050,
       'username': user, 'password': pw}
 
# generating he connection string
connString = 'Driver=%s; Host=%s; Port=%d; Database=default; AuthMech=3; UseSASL=1; UseSystemTrustStore=0;     TrustedCerts=%s; UID=%s; PWD=%s; SSL=1;         AllowSelfSignedServerCert=1;CAIssuedCertNamesMismatch=1' % (cfg['Driver'], cfg['host'], cfg['port'], cert, cfg['username'], cfg['password'])
 
# Create connection
conn = pyodbc.connect(connString, autocommit=True)
 
# Get cursor to interact with the SQL engine
cursor = conn.cursor()
print("Successfully connected to Impala daemon on", hostname)


cursor.execute(query)

cursor.description
field_names = [i[0] for i in cursor.description]
field_names

data=list(cursor.fetchall())
row_as_list = [list(x) for x in data]

df = pd.DataFrame(row_as_list,columns=field_names)

df_new = df 


print("Successfully loaded dataframe")


# In[18]:


print("The shape of the dataframe is : ",df_new.shape)


# In[19]:


print(df_new.head())


# In[13]:


### Get the current date in terms of dt_skey

dates_lst_str = datetime.today().strftime('%Y%m%d')
### Dump onto LOCAL

df_new.to_csv('C:\\Users\\user\\Documents\\Learnings\\scheduling_bras_script\\changes_'+dates_lst_str+'-1100.csv',header=False,index=False)


# In[16]:


### Upload onto the SFTP server: 

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword,port=22, cnopts=cnopts) as sftp:
    print("Connection succesfully stablished ... ")
    remoteFilePath = '/export/home/mission'
    
    with sftp.cd(remoteFilePath):
        print("Entered mission folder ... ")
        # Define the file that you want to upload from your local directorty
        localFilePath = 'C:\\Users\\user\\Documents\\Learnings\\scheduling_script\\changes_'+dates_lst_str+'-1100.csv'

        # Define the remote path where the file will be uploaded
        remoteFilePath = '/export/home/mission'
        
        sftp.put(localFilePath)
        print('Successfully placed the file...{0}'.format('changes_'+dates_lst_str+'-1100.csv')) 
    




