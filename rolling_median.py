# -*- coding: utf-8 -*-
"""
Created on Thu Jul 10 09:24:15 2016
@author: Danylo Zherebetskyy
Reads the .txt-files with strict JSON-data
reads static file
To improve: for very large data-sets (GB) and data streaming, need to 
rewrite code using IJSON 
"""

import sys
import os
import json
import datetime
import numpy as np

def read_input(file_in):
    """Open and read the input JSON-file, return list of dictionaries"""
    if os.stat(file_in).st_size==0:         #check if file is empty
        sys.exit('ERROR: empty input file')
    else:
        file_in=open(file_in, 'r')          #open file
    list_dict=[]
    for line in file_in:
        d=json.loads(line)                  #parse JSON-format 
        list_dict.append(d)

    file_in.close()
    return list_dict                        #return list of dictionaries
    
def time_diff(t1,t2):
    """Calculate time difference between ISO 8601-formatted dates"""
    #parse datatime from a string in ISO 8601-format
    t1f=datetime.datetime.strptime(t1, "%Y-%m-%dT%H:%M:%SZ")    
    t2f=datetime.datetime.strptime(t2, "%Y-%m-%dT%H:%M:%SZ")
    return t2f-t1f

def add_connection(dict_connect,ldi):
    """returns updated dictionary with names as keys and number of connections as values"""
    #add actor/target with value 1 to dictionary if not in dictionary, 
    #or increase number of connections, if they are in dictionary 
    if ldi['actor'] not in dict_connect.keys():     #
        dict_connect[ldi['actor']]=1
    else:
        dict_connect[ldi['actor']]+=1
    if ldi['target'] not in dict_connect.keys():
        dict_connect[ldi['target']]=1
    else:
        dict_connect[ldi['target']]+=1
    return dict_connect 
    
def rm_connection(dict_connect,ldj):
    """removes specified connections in the dictionary"""
    #decrease number of connections for actor and target
    #if 0-connections then remove from the dictionary
    dict_connect[ldj['actor']]-=1
    dict_connect[ldj['target']]-=1
    if dict_connect[ldj['actor']]==0:
        del dict_connect[ldj['actor']]
    if dict_connect[ldj['target']]==0:
        del dict_connect[ldj['target']]
    return dict_connect
    
#def rol_med():
    

def main():
    #check if 3 arguments are sypplied (1-code and 2-files)    
    if len(sys.argv) != 3:
        print 'usage: ./rolling_median.py /path/to/input /path/to/output'
        sys.exit(1)
        
    #use supplied arguments-files
    file_in = sys.argv[1]     #'--count'#sys.argv[1]
    file_out = sys.argv[2]    #'small.txt'#sys.argv[2]
    file_out=open(file_out, 'w+')
    
    #create list of dictionaries from JSON
    ld=read_input(file_in)

    dict_connect={}   #dictionary of names as keys and number of connections as values
    short_ld=[]       #list of dictionaries that are within 60sec timeframe
    
    #initial timeframe: time of first transaction and 60sec before
    t2=datetime.datetime.strptime(ld[0]['created_time'], "%Y-%m-%dT%H:%M:%SZ")
    dt=datetime.timedelta(0, 60)        #60sec time-frame
    t1=t2-dt

    for i in range(len(ld)):
        t=datetime.datetime.strptime(ld[i]['created_time'], "%Y-%m-%dT%H:%M:%SZ")
        #if time of transaction was more then 60 sec before the last one - ignore
        if t<t1:
            #do we need to update median? I assume complete ignore 
            continue
        #if new transaction time is within the frame, just update connections
        if (t1<=t and t<=t2):      
            dict_connect=add_connection(dict_connect,ld[i])
            short_ld.append(ld[i]) 
        #if new transaction time is newer then frame, update bothe the frame and connections 
        elif t>t2:
            t2=t
            t1=t-dt
            dict_connect=add_connection(dict_connect,ld[i])
            short_ld.append(ld[i])
            #check times of old connections and remove ones that are not in new time-frame
            rem=[]
            for j in range(len(short_ld)):
                t_old=datetime.datetime.strptime(short_ld[j]['created_time'], "%Y-%m-%dT%H:%M:%SZ")
                if t_old<t1:
                    rem.append(j)
                    dict_connect=rm_connection(dict_connect,short_ld[j])
            for k in sorted(rem, reverse=True):
                del short_ld[k]

        #calculate median at each step and write to output file with two-decimal precision 
        a='{:.2f}'.format(np.median(dict_connect.values()))
        file_out.write(str(a)+'\n')

    file_out.close()

     
if __name__ == '__main__':
    #Standard boilerplate that calls the main() function
    main()