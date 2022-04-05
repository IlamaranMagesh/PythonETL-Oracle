# PythonETL-Oracle
ETL process in Oracle database using a python library 'Petl'

## About
This repository is to give an idea of how to do extraction, transformation and loading of a given dataset into an Oracle database table.\
As it is in the title, it uses 'Petl' package. There are many other packages out there for ETL processes (pandas,etc) but I thought\
of showing some love to this package.

## Files
- [/logs](https://github.com/IlamaranMagesh/PythonETL-Oracle/tree/master/logs) - This directory is where the error log files be stored
  > *Must have this directory while running the program !*
- [config.py](https://github.com/IlamaranMagesh/PythonETL-Oracle/blob/master/config.py) - Contains all the required configurations for connecting with the oracle database
- [etl.py](https://github.com/IlamaranMagesh/PythonETL-Oracle/blob/master/etl.py) - Main file where all the ETL process occurs
  > This program accepts 3 arguments\
  > [1] - source file/dataset (.xlsx)\
  > [2] - mapping CSV file\
  > [3] - target table name

  Syntax - ``` etl.py [source] [mapping_file] [target_table] ```
  
  ```
    python etl.py test_dataset.xlsx map.txt bill
  ```
  
- [sqlexec.py](https://github.com/IlamaranMagesh/PythonETL-Oracle/blob/master/sqlexec.py) - Executes all SQL commands written in the [sqlcmds.txt](https://github.com/IlamaranMagesh/PythonETL-Oracle/blob/master/sqlcmds.txt)

- [map.txt](https://github.com/IlamaranMagesh/PythonETL-Oracle/blob/master/map.txt) - CSV file which contains the mappings from source dataset to target table
  > For defining mappings to the target table, follow the below order
 
  ``` [source column_name],[target column_name],[target column_datatype],[Null constraint],[Letter case] ```
  
  
   Eg: ```Name,Name,str,Not Null,upper ```
   
   You can skip other mapping constraints except \[target column_datatype]
   
   Eg: ```Name,Name,str,,```
  
- [sqlcmds.txt](https://github.com/IlamaranMagesh/PythonETL-Oracle/blob/master/sqlcmds.txt) - Contains set of SQL commands to be executed in the database
  > Each block of SQL commands should end with ' \\ ' as end of statements.
- [test_dataset.xlsx](https://github.com/IlamaranMagesh/PythonETL-Oracle/blob/master/test_dataset.xlsx) - Source .xlsx dataset file 

## Note
This repo is to show you or give an idea of how 'petl' can be used for etl processes. I have used Oracle as my database and configured as such but you can use other databases as well. You can also change, add mapping definitions and define them to your wish, I have just given some of them. This is same for the source and map file formats but I have used .xlsx and .csv files. Feel free to check and review the codes. If there are any feedbacks, would love to hear them 😄 And let me know if you need to see the more generalized version of this program such that it runs on any databases and have more mapping fucntions than this. 

``` 
Thank you, Peace !
```
