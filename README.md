# SFTP Transfer automation

This project is an initiative to automatically transfer files onto an SFTP server using python script and to schedule the script to run at a specific time during the day using windows scheduler.  

The major components of this project are :
1. Development of python code to perform the following : 
    - Connect to impala to fetch data and convert into CSV file
    - Connect to SFTP server, check the directory structure, and place the CSV file into the required directory 
2. Creation of .bat file to enable running of the python code on shell
3. Scheduling the .bat file using windows scheduler.
    Use the below link to schedule a .bat file using windows task scheduler : 
    [Scheduling a .bat file](https://windowsreport.com/schedule-batch-file-windows/)
