# -*- coding: utf-8 -*-
"""
This script does the ETL process on Oracle database using petl and cx_Oracle.

Requires 3 arguements {1} source_file {2} mapping_file {3} Target_table
"""

import petl
import cx_Oracle
import sys
import config as cf
from difflib import SequenceMatcher as sm
from datetime import datetime,date

    
def isOpen(conn_Object):
    
    # To check if the connecion is still open
    
    try:
        return conn_Object.pin() is None
    except:
        return False

def compare (src_record,last_row='()'):
    
    # To compare the last record in the DB table and the record to be loaded.
    # The ratio of similarity is set to be greater than 0.75,
    # by this the unformatted record in the source table can be compared with 
    # the formatted row in the DB
    
    src_record = str(src_record)[0:-1].lower()  #records from the source file
    last_row =str(last_row)[0:-1].lower()       #last record in the DB
    
    if(sm(None,src_record,last_row).ratio()>0.75):
        return True
    else:
        return False   

def str_to_class(classname):
    
    # To convert string to builtin-datatype class
    
    return getattr(sys.modules['builtins'], classname)

def d_type_change(field_name,d_type):
    
    # To change the attribute datatype to the given datatype 'd_type'
    # Also converts the date to the default oracle datetype 'DD-MON-YY'
    
    if(d_type=='date'):
        fin_table[field_name]= lambda val:val.strftime("%d-%b-%Y").upper()
    else:
        fin_table[field_name]= str_to_class(d_type)

def case_ch(field_name,case):
    
    # To change the case to either upper or lower
    
    fin_table[field_name]= case

def con_close():
    
    # To close the connection to the DB with committing or rolling back the 
    # transcation that has been made
    
    des=input("Commit the transcation or rollback? [c/r]: ")
    if des=='c':
        conn.commit()        
        print("Transcation committed")
        conn.close()
        print("Connection closed")
    elif des =='r':
        conn.rollback()
        print("Transcation rolled back")
        conn.close()
        print("Connection closed")
    else:
        print("Invalid option. Choose 'c' or 'r' ")
        con_close()


try:
    
    #Opening connection and the cursor for SQL execution
    print("Opening Connection...")
    conn = cx_Oracle.connect(cf.oracle["user"],cf.oracle["pass"],cf.oracle["host"])
    print("Connection established")
    cur = conn.cursor()
    
    #Extracting last_row from DB
    cur.execute("select max(ROWNUM) from %s" %sys.argv[3])
    last_rnum = cur.fetchone()[0]
    cur.execute("select * from {0} where ROWID IN(select max(ROWID) from {0})".format(sys.argv[3]))
    last_row = cur.fetchone()
    print("%d rows are present in the database table" %(lambda x: last_rnum if(last_rnum) else 0)(last_rnum))
    print("'%s' is the last row in the database table" %(lambda x: (last_row,) if(last_row) else '')(last_row))
    
    #Extraction         
    source = petl.fromxlsx(sys.argv[1])
    map_file = petl.fromcsv(sys.argv[2])
    
    #transformation
    hdr_map = []
    for i in map_file:
        hdr_map.append(i[1].upper())
        
    for i in source[0]:
        flag = 0
        for j in map_file:
            if(i == j[0]):
                flag= 1
                break
        if(flag == 0):
            source = petl.cutout(source,i)
            
    src_records = source.records().list()
    temp = []
    for i in range(-1,(-len(src_records)-1),-1):
        if(compare(src_records[i], last_row)): 
            break
        else:
            temp.insert(i,src_records[i]) 
            
    source = petl.pushheader(temp, hdr_map)
    fin_table = petl.convert(source)
    for line in map_file:
        field_name = line[1].upper()
        d_type_change(field_name,line[2])
        if(line[4] != ''):
            case_ch(field_name,line[4])
        
             
    #Loading
    print("Loading the data...")
    petl.appenddb(fin_table, cur, sys.argv[3], commit=False)
    print("Data Loaded...")
    
    #BatchError Handling
    if (cur.getbatcherrors()):
        with open(r"logs\{}_trnslog.txt".format(date.today()),'a+') as file:
            for i in cur.getbatcherrors():
                error = "%s %s %d \n" %(datetime.now().strftime("[%Y-%m-%d::%H:%M:%S]"),i.message,i.offset+2)
                file.writelines(error)
            print("Could not load some records. Logged at",file.name)  
    con_close()

#db_Error Handling
except cx_Oracle.Error as e:
    er, =e.args
    print("DB_Error Occured:",er.code,er.message)
    
    with open(r"logs\{}_dblog.txt".format(date.today()),'a+') as file:
        error ="%s %s \n" %(datetime.now().strftime("[%Y-%m-%d::%H:%M:%S]"),er.message)
        file.writelines(error)
        print("Error has been logged at",file.name)

#Other_Error Handling    
except Exception as e:
    print("Error Occured:",e)
    print("Closing Connection...")  
    
finally:
    try:
        if (isOpen(conn)):
            conn.close()
    except:
        pass
 
 
