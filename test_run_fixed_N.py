#!/usr/bin/env python
"""
@author: chwala-c
"""

import sys
import psutil
from multiprocessing import Queue, Process, Event
from time import sleep
from subprocess import Popen, PIPE

sys.path.insert(0, '/home/chwala-c/code/pySNMPdaq/')
from pySNMPdaq import SnmpDAQSession

def get_number_of_file_descriptors():
    p1 = Popen(['lsof', '-l'], stdout=PIPE)
    p2 = Popen(['wc', '-l'], stdin=p1.stdout, stdout=PIPE)
    return p2.communicate()[0]

def test_run(N):

    mem_before = psutil.virtual_memory().used/1e9

    snmpDAQSessions = []
    query_results_queue = Queue()
        
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
    mem_during = psutil.virtual_memory().used/1e9

    #print get_number_of_file_descriptors()
    
    sleep(1)
    
    for session in snmpDAQSessions:
        #session.stop_listener()
        #print 'Terminating SNMP query processe...'
        session.listener_process.terminate()
        #print 'Joining SNMP query processe...'
        session.listener_process.join()
        
    mem_after = psutil.virtual_memory().used/1e9
        
    return mem_before, mem_during, mem_after

def implementation_1(N):
    class my_foo_class():
        def __init__(self):
            self.foo = 1
        def query():
            # Do something
            a = 1+1
            return a
        
    def worker(queue_in, queue_out):
        from os import getpid
        pid = getpid()
        snmp_session = 'some session'
        command = queue_in.get()
        #while True:
        #    sleep(1)      
    
    mem_before = psutil.virtual_memory().used/1e9    
    
    main_output_queue = Queue()
    trigger_queue_list = []
    jobs = []    
    
    for i in range(N):
        trigger_queue = Queue()
        trigger_queue_list.append(trigger_queue)
        p = Process(target=worker, args=(trigger_queue, main_output_queue))
        jobs.append(p)
        p.start()
    mem_during = psutil.virtual_memory().used/1e9
        
    sleep(0.1)
            
    for i in range(N):
        p = jobs.pop()
        p.terminate()
    mem_after = psutil.virtual_memory().used/1e9

    return mem_before, mem_during, mem_after

def implementation_2(N):
    class my_foo_class():
        def __init__(self):
            self.foo = 1
        def query():
            # Do something
            a = 1+1
            return a
        
    def worker(trigger_event, queue_out):
        from os import getpid
        pid = getpid()
        snmp_session = 'some session'
        trigger_event.wait()
        #while True:
        #    sleep(1)      
    
    mem_before = psutil.virtual_memory().used/1e9    
    
    main_output_queue = Queue()
    trigger_event_list = []
    jobs = []    
    
    for i in range(N):
        trigger_event = Event()
        trigger_event_list.append(trigger_event)
        p = Process(target=worker, args=(trigger_event, main_output_queue))
        jobs.append(p)
        p.start()
    mem_during = psutil.virtual_memory().used/1e9
        
    sleep(0.1)
            
    for i in range(N):
        p = jobs.pop()
        p.terminate()
    mem_after = psutil.virtual_memory().used/1e9

    return mem_before, mem_during, mem_after

def implementation_3(N):
    class my_foo_class():
        def __init__(self):
            self.foo = 1
        def query():
            # Do something
            a = 1+1
            return a
        
    def worker(queue_out):
        from os import getpid
        pid = getpid()
        snmp_session = 'some session'
        while True:
            sleep(1)      
        
    mem_before = psutil.virtual_memory().used/1e9    
    
    main_output_queue = Queue()
    jobs = []    
    
    for i in range(N):
        p = Process(target=worker, args=(main_output_queue,))
        jobs.append(p)
        p.start()
    mem_during = psutil.virtual_memory().used/1e9
        
    sleep(0.1)
            
    for i in range(N):
        p = jobs.pop()
        p.terminate()
    mem_after = psutil.virtual_memory().used/1e9

    return mem_before, mem_during, mem_after

def implementation_4(N):
    class my_foo_class():
        def __init__(self):
            self.foo = 1
        def query():
            # Do something
            a = 1+1
            return a
        
        def worker(self, trigger_event, queue_out):
            from os import getpid
            pid = getpid()
            snmp_session = 'some session'
            trigger_event.wait()
            #while True:
            #    sleep(1)      
    
    mem_before = psutil.virtual_memory().used/1e9    
    
    main_output_queue = Queue()
    trigger_event_list = []
    jobs = []    
    foo_objs = []
    
    for i in range(N):
        trigger_event = Event()
        trigger_event_list.append(trigger_event)
        foo_obj = my_foo_class()
        p = Process(target=foo_obj.worker, args=(trigger_event, main_output_queue))
        jobs.append(p)
        foo_objs.append(foo_obj)
        p.start()
    mem_during = psutil.virtual_memory().used/1e9
        
    sleep(0.1)
            
    for i in range(N):
        p = jobs.pop()
        p.terminate()
    mem_after = psutil.virtual_memory().used/1e9

    return mem_before, mem_during, mem_after

def implementation_5(N):
    class my_foo_class():
        def __init__(self):
            self.foo = 1
        def query():
            # Do something
            a = 1+1
            return a
        
    def worker(queue_in, queue_out):
        from os import getpid
        pid = getpid()
        snmp_session = 'some session'
        command = queue_in.get()
        #while True:
        #    sleep(1)      
    
    mem_before = psutil.virtual_memory().used/1e9    
    
    main_output_queue = Queue()
    trigger_queue = Queue()
    jobs = []    
    
    for i in range(N):
        p = Process(target=worker, args=(trigger_queue, main_output_queue))
        jobs.append(p)
        p.start()
    mem_during = psutil.virtual_memory().used/1e9
        
    sleep(0.1)
            
    for i in range(N):
        p = jobs.pop()
        p.terminate()
    mem_after = psutil.virtual_memory().used/1e9

    return mem_before, mem_during, mem_after


###############
### M A I N ###
###############

implementation_id = sys.argv[1]
N = int(sys.argv[2])

implementations = {'0': test_run,
                   '1': implementation_1,
                   '2': implementation_2,
                   '3': implementation_3,
                   '4': implementation_4,
                   '5': implementation_5}

mem_before, mem_during, mem_after = implementations[implementation_id](N)

print '%04.0d, %04.0d, %2.3f, %2.3f, %2.3f' % (int(implementation_id),
                                       N, 
                                               mem_before,
                                               mem_during,
                                               mem_after)