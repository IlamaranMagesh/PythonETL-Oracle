# -*- coding: utf-8 -*-
"""

Execute set of SQL commands in oracle database
"""

import os                      # For creating dirs
import cx_Oracle               # To connect with the Oracle DB
import config  as cf           # Contians all the credentials to connect
from datetime import datetime  # To get the time and date for our error-logging


try:
    
    # Opening connection with the oracle using the config file
    print("Opening Connection...")
    orcl_con = cx_Oracle.connect(cf.oracle["user"],cf.oracle["pass"],cf.oracle["host"])
    
    # Opening cursor in the DB connection for executing SQL commands
    cur=orcl_con.cursor()
    print("Connection Successful !")
    
    # Opening the SQL command text file in appending mode
    with open('sqlcmds.txt') as file:
        
        lines = file.read()
        if(lines):
        
            #Splitting the string into list of strings, with '/' as delimiter
            lines = lines.split('/')[0:-1] #removing the empty lines at the end of the file
                
            # Executing each lines in the file
            for i,line in enumerate(lines):
            
                # Stripping out '\n' if present in the line, Since it causes error while execution
                line = line.strip('\n;')
                cur.execute(line)
                print("Statement {} executed...".format(i+1))
        
            print("Execution Successful...")
        
        else:
            print("File is empty...")
            
except Exception as e:
    
    #Creating Log dir
    os.makedirs(os.getcwd()+"\logs",exist_ok=True)
    
    # Logging the errors, if any occurred
    with open('logs\exec_log.txt','a+') as file:
        error="%s %s\n" %(datetime.now(),e)
        file.writelines(error)
    
    print("\nError occured: see the log file\n")
        
finally:
    
    # Closing the cursor and the connection
    print("Closing Connection...")
    cur.close()
    orcl_con.close()
    print("Connection Closed !")
