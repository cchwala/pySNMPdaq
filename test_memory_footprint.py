#!/usr/bin/env python
"""
Created on Wed Jan 20 15:34:11 2016

@author: chwala-c
"""

import sys
import psutil
from multiprocessing import Queue
from time import sleep
from datetime import datetime
from subprocess import Popen, PIPE

import numpy as np

sys.path.insert(0, '/home/chwala-c/code/pySNMPdaq/')
from pySNMPdaq import SnmpDAQSession

import matplotlib.pyplot as plt

# Values for old version of pySNMPdaq
old_N_list = [10, 100, 200, 300]
old_max_mem_list = [3.14644479, 3.5717939, 5.0639380, 6.8758446]

N_list = [600,]
max_mem_list = []

def get_number_of_file_descriptors():
    p1 = Popen(['lsof', '-l'], stdout=PIPE)
    p2 = Popen(['wc', '-l'], stdin=p1.stdout, stdout=PIPE)
    return p2.communicate()[0]

for N in N_list:

    print N

    snmpDAQSessions = []
    query_results_queue = Queue()
    
    t_list = []
    mem_list = []
    
    #print get_number_of_file_descriptors()
    
    for i in range(N):
        session = SnmpDAQSession(IP='123.321.123.321',
                                ID='foobar' + str(i),
                                oid_dict={'foo':'1.3.6.1.2.1.1.3'},
                                WRITE_TO_FILE=False,
                                WRITE_TO_STDOUT=False,
                                timeout=1*1000000, # microseconds
                                retries=2,
                                query_results_queue=query_results_queue)
        session.start_listener()
        snmpDAQSessions.append(session)
    
        t_list.append(datetime.utcnow())
        mem_list.append(psutil.virtual_memory().used/1e9)

    #print get_number_of_file_descriptors()
    
    #print 'Sleeping...'
    sleep(1)
    #print 'Going on...'
    
    #fig, ax = plt.subplots()
    #ax.plot(t_list, mem_list)
    #plt.title(str(N))
    #plt.show()
    
    
    for session in snmpDAQSessions:
        #session.stop_listener()
        #print 'Terminating SNMP query processe...'
        session.listener_process.terminate()
        #print 'Joining SNMP query processe...'
        session.listener_process.join()
        
        t_list.append(datetime.utcnow())
        mem_list.append(psutil.virtual_memory().used/1e9)
        
    max_mem_list.append(np.array(mem_list).max())



fig, ax = plt.subplots()
#ax.plot(t_list, mem_list)
ax.plot(old_N_list, old_max_mem_list, '-o')
ax.plot(N_list, max_mem_list, '-o')
plt.show()

print N_list
print max_mem_list
    
