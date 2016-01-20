# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 13:16:15 2014

@author: chwala-c
"""

import netsnmp
import signal
import logging
import errno
from os import getpid, makedirs, path
from multiprocessing import Event, Process, Queue
from datetime import datetime
from time import sleep, time

import numpy as np
import pandas as pd

DATA_FILE_FORMAT_VERSION = '1.0'

def pySNMPdaq_loop():
    ''' 
    The main loop for pySNMPdaq that is run till infinity 

    This function takes care of all the initialization for the startup
    and all the cleaning up for the termination of pySNMPdaq. It can be
    called directly. Then it is terminated by Ctrl+C. Or it can be 
    called as the run method of a daemon. Then it is terminated by SIGTERM.

    It reads the config from the local file config.py and the list of IPs
    and OIDs from mw_link_OID_listing.py.        
    
    '''

    # Parse configuration
    import config    

    # Start logging
    make_sure_path_exists(config.LOG_DIR, do_logging=False)
    logging.basicConfig(filename=path.join(config.LOG_DIR, config.LOG_FILE),
                        level=config.LOG_LEVEL,
                        format='%(asctime)s %(message)s')

    logging.info('###===================###')                        
    logging.info('### pySNMPdaq started ###')
    logging.info('###===================###')
    logging.debug('Started main loop with PID %d', getpid())

    # Parse list of MW links with the SNMP query OIDs
    import mw_link_OID_listing
    logging.debug('MW link OID listing imported')

    # Create data directories
    make_sure_path_exists(config.DATA_DIR)
    make_sure_path_exists(path.join(config.DATA_DIR,
                                    config.CONFIG_ARCHIVE_DIR))
    if config.ARCHIVE_FILES:
        make_sure_path_exists(path.join(config.DATA_DIR, 
                                        config.ARCHIVE_DIR))
    if config.PUT_DATA_TO_OUT_DIR:
        make_sure_path_exists(path.join(config.DATA_DIR, 
                                        config.DATA_OUT_DIR))
    if config.SSH_TRANSFER:
        make_sure_path_exists(path.join(config.DATA_DIR, 
                                        config.SSH_REFUGIUM_DIR))

    # Save a timestamped copy of the config file
    config_file_name = save_timestamped_config_and_mw_links_list(
                            config,
                            mw_link_OID_listing)
    logging.info('Current config file: %s', config_file_name)

    # TODO: 
    #  -Refactor startup of all Processes to avoid the mess below... 
    #  -Refactor timers so that they are seperated
    #
    
    # Init timers    

    # Init dataHandler

    # Init SNMPdaqSessions

    # Connect timers to message queues

    # ------------------------------------
    # Messy startup
    from multiprocessing import Queue
    from pySNMPdaq import SnmpDAQSession, DataHandler, SessionTimer
    
    query_results_queue = Queue()
    new_file_trigger_queue = Queue()
    
    snmpDAQSessions = []
    
    # Init a SNMP-DAQ session for each link
    for link in mw_link_OID_listing.mw_link_list:
        try:
            snmpDAQSessions.append(
                SnmpDAQSession(IP=link['IP'],
                               ID=link['ID'],
                               oid_dict=link['OID_dict'],
                               SNMP_VERSION=config.SNMP_VERSION,
                               community=config.SNMP_COMMUNITY,
                               password=config.SNMP_AUTHPASSWORD,
                               username=config.SNMP_USERNAME,
                               WRITE_TO_FILE=config.WRITE_TO_FILE,
                               WRITE_TO_STDOUT=config.WRITE_TO_STD_OUT,
                               timeout=config.SNMP_TIMEOUT_SEC*1000000,
                               retries=config.SNMP_RETRIES,
                               query_results_queue=query_results_queue))
            logging.debug('Started snmpDAQSession for %s', 
                          link['ID'])
        except:
            logging.warning('Could not start snmpDAQSession for %s', 
                            link['ID'])

    # Start the queue listener processes
    for snmpDAQSession in snmpDAQSessions:
        snmpDAQSession.start_listener()
    
    # Init DataHandler
    dataHandler = DataHandler(query_results_queue=query_results_queue,
                              new_file_trigger_queue=new_file_trigger_queue,
                              data_dir=config.DATA_DIR,
                              filename_prefix=config.FILENAME_PREFIX,
                              current_config_file=config_file_name,
                              archive_files=config.ARCHIVE_FILES,
                              archive_dir=config.ARCHIVE_DIR,
                              put_data_to_out_dir=config.PUT_DATA_TO_OUT_DIR,
                              data_out_dir=config.DATA_OUT_DIR,
                              ssh_transfer=config.SSH_TRANSFER,
                              ssh_user=config.SSH_USER,
                              ssh_server=config.SSH_SERVER,
                              ssh_remotepath=config.SSH_REMOTEPATH,
                              ssh_refugium_dir=config.SSH_REFUGIUM_DIR)
    dataHandler.start()
    
    # Init timer
    sessionTimer = SessionTimer(trigger_wait_sec=config.TIMER_1_QUERY_WAIT_SEC,
                                SnmpDAQSessions=snmpDAQSessions, 
                                new_file_trigger_queue=new_file_trigger_queue)
    sessionTimer.start();
    
    #----------------------------------------
    # Messy startup end    

    # Define signal handler for SIGTERM which is sent to stop loop
    # when running as daemon.        
    signal.signal(signal.SIGTERM, sigterm_handler)

    # Start infinite loop. It can be exited by raising SystemExit, 
    # which is done in the signal handler. The 'finally' part than
    # takes care of stopping all the processes and clean up.
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print 'main() received Ctrl+C'
        for session in snmpDAQSessions:
            print 'Terminating SNMP query processe...'
            session.listener_process.terminate()
            print 'Joining SNMP query processe...'
            session.listener_process.join()
        print 'Exiting timer session'
        sessionTimer.stop()
        print 'Exiting data handler'
        dataHandler.stop()
    finally:
        logging.debug('Daemon is trying to clean up!')
        for session in snmpDAQSessions:
            #print 'Terminating SNMP query processe...'
            session.listener_process.terminate()
            #print 'Joining SNMP query processe...'
            session.listener_process.join()
        logging.debug('Trying to stop timer process...')
        sessionTimer.stop()
        logging.debug('Trying to stop dataHandler processes...')
        dataHandler.stop()
        print 'Exit!'
 
 
def sigterm_handler(signal, frame):
    ''' Handler function to catch SIGTERM and trigger clean exit '''
    logging.debug('pySNMPdaq got SIGTERM')
    import sys
    # Raise SystemExit. That causes the main loop to switch to
    # the 'finally' after the 'try'
    sys.exit(0)


def make_sure_path_exists(path, do_logging=True):
    ''' Create a path if it does not exist '''
    try:
        makedirs(path)
        if do_logging == True:
            logging.debug('Created %s', path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            logging.debug('Creation of %s went wrong', path)
            raise
        else:
            if do_logging == True:
                logging.debug('Path %s already existed', path)


def save_timestamped_config_and_mw_links_list(config, mw_link_OID_listing):
    ''' Save pySNMPdaq config and MW link OID listing in a unified file '''
    import inspect    
    
    # Build filename for unified config file
    filename = ('config_' + config.FILENAME_PREFIX + '_'
                          + datetime.utcnow().strftime('%Y%m%d_%H%M') 
                          + '.py')
    # Build absolute filename for unified config file
    absfilename = path.join(config.DATA_DIR, 
                            config.CONFIG_ARCHIVE_DIR,
                            filename)
    
    # Get absolute filename from module 'config'
    fn_config = inspect.getabsfile(config)
    logging.debug('Config filename to read from %s', 
                  fn_config)
    # Get absolute filename from module 'mw_link_OID_listing'
    fn_mw_link_OID_listing = inspect.getabsfile(mw_link_OID_listing)
    logging.debug('mw_link_OID_listing filename to read from %s', 
                  fn_mw_link_OID_listing)
    
    # Concatenate the content of the two files into one unified config file
    with open(absfilename, "wb") as outfile:
        for f in [fn_config, fn_mw_link_OID_listing]:
            with open(f, "rb") as infile:
                outfile.write(infile.read())
                outfile.write('\n\n') # add some vertical space
    logging.info('Config written to %s', absfilename) 
    
    # Put a copy of the config file to the data outbox dir (if desired)
    if config.PUT_DATA_TO_OUT_DIR:
        import shutil
        shutil.copy(absfilename, path.join(config.DATA_DIR, 
                                           config.DATA_OUT_DIR))
    
    # Return the filename of the unified config file
    return filename


def build_empty_mw_link_record_array(oid_dict):
    ''' Build a numpy record array to store MW link query '''
    dtype_list = [('Timestamp_UTC', 'datetime64[us]'), 
                  ('MW_link_ID', object),
                  ('RTT', np.float64)]
    for oid_name in oid_dict:
        if (oid_name != 'RX_level' and oid_name != 'TX_level'):
            dtype_list.append((oid_name, np.float64))
    mw_link_record = np.empty((1, ), dtype=dtype_list)
    
    # Set all numeric entries to NaN
    mw_link_record['RTT'] = np.NaN
    for oid in oid_dict:
        mw_link_record[oid] = np.NaN
    return mw_link_record


def build_str_from_mw_link_record(mw_link_record):
    ''' Build a nicely formated string for a MW link record '''
    s = ''
    s += mw_link_record['Timestamp_UTC'][0].astype(
                                        datetime).strftime(
                                          '%Y-%m-%d %H:%M:%S')
    s += ', '
    s += str(mw_link_record['MW_link_ID'][0]).ljust(17)
    s += ', '
    s += str(mw_link_record['RTT'][0].round(4)).ljust(8)
    s += ', '
    for oid_name in mw_link_record.dtype.names[3:]:
        s += str(mw_link_record[oid_name][0]).rjust(12)
        s += ', '
    return s[:-2]

def reorder_columns_of_DataFrame(df, order=['MW_link_ID', 'RTT']):
    ''' Reorder columns of a Pandas DataFrame '''
    columns = df.columns.tolist()
    ordered_columns = []
    for item in order:
        columns.remove(item)
        ordered_columns.append(item)
    ordered_columns += columns
    return df[ordered_columns]

class SnmpDAQSession():
    '''SNMP Session for polling data from a MW Link'''
    def __init__(self, 
                 IP,
                 ID,
                 oid_dict,
                 query_results_queue=Queue(),
                 SNMP_VERSION=1, community='public',
                 username=None, password=None,
                 timeout=2000000, retries=1,
                 WRITE_TO_STDOUT=True, 
                 WRITE_TO_FILE=False, 
                 GROW_DATA_RECORD=False):
                             
        self.data_reccord = []
        self.IP = IP
        self.ID = ID
        self.SNMP_VERSION = SNMP_VERSION
        self.community = community
        self.username = username
        self.password = password
        self.timeout = timeout
        self.retries = retries
        self.WRITE_TO_STDOUT = WRITE_TO_STDOUT
        self.WRITE_TO_FILE = WRITE_TO_FILE
        self.GROW_DATA_RECORD = GROW_DATA_RECORD

        self.oid_dict = oid_dict
        self.oid_list = oid_dict.values()
        self.oid_name_list = oid_dict.keys()
        self.mw_link_record = build_empty_mw_link_record_array(oid_dict)        
        
        self.trigger_queue = Queue()
        
        self._is_idle = True
        #self._is_idle.set()

        self.var_list = netsnmp.VarList()
        for oid in self.oid_list:
            #logging.debug(' for IP ' + str(self.mwLinkSite.IPaddress) + ' appending to var_list: ' + oid)
            self.var_list.append(netsnmp.Varbind(oid))

        # Queue for pushing data to other processes (e.g. data handler)        
        self.query_results_queue = query_results_queue
        
        # Process which listens for incoming triggers in trigger queue
        self.listener_process = Process(target=self._listener_loop)
    
    def start_listener(self):
        # Start process that listens for query triggers 'TRIGGER'        
        self.listener_process.start()

    def stop_listener(self):
        self.trigger_queue.put('STOP')

    def _listener_loop(self):  
        # Ignore SIGINT to be able to handle it in
        # main() and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.debug('Started SNMPSession listener loop with PID %d', getpid())
        message = self.trigger_queue.get()
        while message != 'STOP':
            if message == 'TRIGGER':
                self._query()
            message = self.trigger_queue.get()
        logging.debug('=== ' + str(self.ID) + 
                      ' received STOP. Exiting listener loop... ===')

    def _query(self):
        # Tell everybody that we are busy now
        self._is_idle = False
        #logging.debug(' busy')
        #print self.IP + ' BUSY'
                
        # Init SNMP Session for different protocol versions
        if self.SNMP_VERSION == 1 or self.SNMP_VERSION == 2:
            logging.debug(' open session with SNMPv1 for ' + self.IP)
            SnmpSession = netsnmp.Session(DestHost=self.IP,
                                               Version=self.SNMP_VERSION,
                                               Community=self.community,
                                               Timeout=self.timeout,
                                               Retries=self.retries)
        if self.SNMP_VERSION == 3:
            logging.debug(' open session with SNMPv3 for ' + self.IP)
            SnmpSession = netsnmp.Session(DestHost=self.IP,
                                               Version=self.SNMP_VERSION,
                                               SecName=self.username,
                                               AuthPass=self.password,
                                               AuthProto='MD5',
                                               PrivProto='DES',
                                               SecLevel='authNoPriv',
                                               Timeout=self.timeout,
                                               Retries=self.retries)
        # Start time for round trip time
        t1 = time()
        # Send SNMP query
        self.queryResult = SnmpSession.get(self.var_list)
        # Get round trip time (RTT)
        self.queryRTT = time() - t1        
        # Put queryResults, RTT and a timestamp in mw_link_record
        self._fill_mw_link_record()    
        # Stream data to data handler processes        
        self._streamQueryResult()
        
        #
        # WORKAROUND FOR PROBLEM WITH REPEATED QUERIES OF 
        # Tx-, Rx-, etc. data --> renew varlist
        #   
        # IS THIS STILL NECESSARY WHEN USING OTHER MW LINKS ???!!
        #
        var_list = netsnmp.VarList()
        for oid in self.oid_list:
            #logging.debug(' for IP ' + str(self.mwLinkSite.IPaddress) + ' appending to var_list: ' + oid)
            var_list.append(netsnmp.Varbind(oid))
        self.var_list = var_list

        # Tell everybody that we are idle now        
        self._is_idle = True

        #logging.debug(' idle')
        #print self.IP +  ' IDLE'

    def _fill_mw_link_record(self):
        self._set_mw_link_record_time()
        self._write_query_result_to_mw_link_record()
        self.mw_link_record['MW_link_ID'] = self.ID
        

    def _set_mw_link_record_time(self):
        self.mw_link_record['Timestamp_UTC'] = np.datetime64(
                                                    datetime.utcnow(),'us')
        self.mw_link_record['RTT'] = self.queryRTT

    def _write_query_result_to_mw_link_record(self):
        ''' Write SNMP query to mw_link_record according to OIDs '''
        for i, oid_name in enumerate(self.oid_name_list):
            #print self.IP, self.oid_name_list, self.queryResult, i, self.queryResult[i]
            #print self.IP, self.mw_link_record
            if self.queryResult[i] != None:
                self.mw_link_record[oid_name] =  self.queryResult[i]
            else:
                self.mw_link_record[oid_name] =  np.NaN

    def _stream_mw_link_record(self):
        pass

    def _streamQueryResult(self):
        record_str = build_str_from_mw_link_record(self.mw_link_record)
        if self.WRITE_TO_STDOUT:
            print record_str
        if self.WRITE_TO_FILE:
            self.query_results_queue.put(self.mw_link_record)
        if self.GROW_DATA_RECORD:
            pass
            # CHANGE THIS TO FILL THE MW_LINK_PARAMETER
            #for result in self.queryResult:
            #    s = s + ' ' + str(result)       
            #self.data_record.append(s)            
            
class DataHandler():
    '''Data handler which will be looped in a seperate process to manage 
       the data streams from the SNMP queries'''
    def __init__(self,
                query_results_queue=None,
                new_file_trigger_queue=None,
                data_dir='data',
                filename_prefix='mw_link_queries',
                current_config_file='foobar.baz',
                archive_files=False,
                archive_dir='ARCHIVE',
                put_data_to_out_dir=False,
                data_out_dir='DATA_OUT',
                ssh_transfer=False,
                ssh_user='user',
                ssh_server='server.com',
                ssh_remotepath='some_dir',
                ssh_refugium_dir='REFUGIUM'):
        self.query_results_queue = query_results_queue
        self.new_file_trigger_queue = new_file_trigger_queue
        self.data_dir = data_dir
        self.filename_prefix = filename_prefix
        self.current_config_file = current_config_file

        self.archive_files = archive_files
        self.archive_dir = archive_dir
        self.put_data_to_out_dir = put_data_to_out_dir
        self.data_out_dir = data_out_dir
        self.ssh_transfer = ssh_transfer
        self.ssh_user = ssh_user
        self.ssh_server = ssh_server
        self.ssh_remotepath = ssh_remotepath
        self.ssh_refugium_dir = ssh_refugium_dir

        self.KEEP_RUNNING = Event()
        self.NEW_DataFrame = Event()
        self.WRITE_TO_FILE = Event()               
        
        self.KEEP_RUNNING.set()
        
        self._open_new_file()
        self.NEW_DataFrame.set()
        
        self.ssh_filename_queue = Queue()
        
        self.listener_loop_process = Process(
                                    target=self._listener_loop)
        self.data_handler_loop_process = Process(
                                    target=self._data_handler_loop)
        self.ssh_loop_process = Process(
                                    target=self._ssh_loop)

    def _listener_loop(self):
        # Ignore SIGINT to be able to handle it in 
        # main() and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.debug('Started DataHandler listener loop with PID %d', getpid())
        while self.KEEP_RUNNING.is_set():
            logging.debug('_listener_loop is waiting for message')
            message = self.new_file_trigger_queue.get()
            logging.debug('_listener_loop got message %s', message)
            if message == 'EXIT':
                break
            if message == 'WRITE_TO_FILE':
                self.WRITE_TO_FILE.set()
                # give the data handler some time to write 
                # DataFrame to file and to initialize new DataFrame
                sleep(2)
                # then check if the WRITE_TO_FILE is cleared, 
                # that is, the file handler has opend new file
                if self.WRITE_TO_FILE.is_set():
                    logging.warning('File handler did not clear WRITE_TO_FILE Event')
        logging.debug('Exit DataHandler._listener_loop')

    def _data_handler_loop(self):
        # Ignore SIGINT to be able to handle it in 
        # main() and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.debug('Started DataHandler data_handler loop with PID %d', getpid())
        while self.KEEP_RUNNING.is_set():
            sleep(0.01)
            if self.WRITE_TO_FILE.is_set():
                # wirte to file, call for new DataFrame,
                # close old file and open new one
                self._write_mw_link_record_to_file()
                self.NEW_DataFrame.set()
                self._close_file()
                self._open_new_file()
                
                self.WRITE_TO_FILE.clear()

            # get data from queue and write to DataFrame which will
            # be writen to file when WRITE_TO_FILE Event is set
            while self.query_results_queue.empty() == False:
                self.record = self.query_results_queue.get()
                if self.NEW_DataFrame.is_set():
                    self.df = pd.DataFrame(self.record)
                    self.df.set_index('Timestamp_UTC', inplace=True)
                    self.NEW_DataFrame.clear()
                    logging.debug('----- NEW DataFrame ----')
                else:
                    df_temp = pd.DataFrame(self.record)                
                    df_temp.set_index('Timestamp_UTC', inplace=True)                    
                    self.df = self.df.append(df_temp)
                    self.df = reorder_columns_of_DataFrame(self.df)
                    #print ' grown DataFrame \n' + str(self.df.tail(1))
        logging.debug('Exit DataHandler._data_handler_loop')

    def _ssh_loop(self):
        from os import path
        # Ignore SIGINT to be able to handle it in 
        # main() and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.debug('Started DataHandler ssh_loop with PID %d', getpid())
        while self.KEEP_RUNNING.is_set():
            message = self.ssh_filename_queue.get()
            logging.debug('_ssh_loop got message %s',message)

            if message == 'EXIT':
                break
            else:
                # queue item should be filename
                filename = message
                # and copy it via scp (this function also takes care of ssh
                # timeouts and temporarily stores files in a REFUGIUM dir)
                copy_file_via_scp(filename=filename,
                                  user=self.ssh_user,
                                  server=self.ssh_server,
                                  remotepath=self.ssh_remotepath,
                                  move_to_refug=True,
                                  check_refug=True,
                                  refug_dir=path.join(self.data_dir,
                                                      self.ssh_refugium_dir),
                                  move_to_archive=self.archive_files,
                                  archive_dir=path.join(self.data_dir,
                                                        self.archive_dir))
        logging.debug('Exit DataHandler._ssh_loop')

          
    def _open_new_file(self):
        from os import path
        filename = path.join(self.data_dir,  
                             self.filename_prefix
                             + '_'
                             + datetime.utcnow().strftime('%Y%m%d_%H%M') 
                             + '.tmp')
        self.data_file = open(filename, 'w')
        logging.debug(' opend data file ' 
                     + self.data_file.name)
        self.data_file.write('# File format version: ' 
                            + DATA_FILE_FORMAT_VERSION
                            + '\n')
        self.data_file.write('# Opend ' 
                            + datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                            + ' UTC\n')
        self.data_file.write('# Associated config file: '
                            + self.current_config_file
                            + '\n')
        self.data_file.flush()

    def _close_file(self):
        from os import rename, path, remove
        # close file
        self.data_file.write('# Closed ' 
                            + datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                            + ' UTC\n')
        self.data_file.close()
        # rename file from .tmp to .dat
        tmp_filename = self.data_file.name
        new_filename = tmp_filename[:-4] + '.dat'
        rename(tmp_filename, new_filename)
        logging.debug(' closed data file ' + self.data_file.name)
        gz_filename = gzip_file(new_filename, delete_source_file=True)
        
        # copy file via ssh
        if self.ssh_transfer==True:
            self.ssh_filename_queue.put(gz_filename)
            
        # copy file to data outbox directory
        if self.put_data_to_out_dir==True:
            from shutil import copy
            # Strip the data path from gzip-ed filename)           
            fn = path.split(gz_filename)[1]
            fn_destination = path.join(self.data_dir, 
                                       self.data_out_dir, 
                                       fn)
            # Move file (and overwrite if neccessary)
            if path.exists(fn_destination):
                remove(fn_destination)           
                logging.warning('overwritting %s ', fn_destination)
            logging.debug('Copying file %s to data outbox',  gz_filename)
            copy(gz_filename, path.join(self.data_dir, self.data_out_dir))            

        # or just move file to archive directory
        if self.archive_files==True:
            try:            
                fn_patern = self.filename_prefix + '_%Y%m%d_%H%M.dat.gz'
                move_timestamped_file_to_folder_struct(
                    filename=path.split(gz_filename)[1],
                    file_dir=path.split(gz_filename)[0],
                    filename_pattern=fn_patern,
                    folder_struct_root_dir=path.join(self.data_dir, 
                                                     self.archive_dir))
            except:
                logging.warn('Could not move file to folder structure')
        
        # or do nothing        
        else:
            pass

    def _write_mw_link_record_to_file(self):
        self.df.to_csv(self.data_file, 
                       na_rep='NaN', 
                       float_format='%.3f')

    def _move_files_to_archive(self):
        pass

    def start(self):
        self.listener_loop_process.start()
        self.data_handler_loop_process.start()
        self.ssh_loop_process.start()

    def stop(self):
        logging.debug('DataHandler.stop: clearing KEEP_RUNNIG')
        self.KEEP_RUNNING.clear()
        #sleep(1)        
        
        # Write out last data to file
        try:
            self._write_mw_link_record_to_file()
        except:
           logging.warning('DataHandler.stop: could not write file')
        try:
            self._close_file()
        except:
            logging.warning('DataHandler.stop: could not close file')

        # Send EXIT message via queues and terminate processes
        logging.debug('DataHandler.stop: sending EXIT')
        self.ssh_filename_queue.put('EXIT')
        self.new_file_trigger_queue('EXIT')
        sleep(0.1)
        self.ssh_loop_process.terminate()
        self.listener_loop_process.terminate()
        
        logging.debug('DataHandler.stop: terminate data_handler_loop process')
        # This one exits without putting something to its queue        
        self.data_handler_loop_process.terminate()
        
        self.listener_loop_process.join()
        self.data_handler_loop_process.join()
        self.ssh_loop_process.join()
        

#
# Some helper functions for file handling
#
import shutil
import os

def move_timestamped_file_to_folder_struct(filename,
                                           file_dir,
                                           filename_pattern,
                                           folder_struct_root_dir,                                           
                                           folder_struct='%Y/%m/%d',
                                           overwrite=False,
                                           erase_if_exists=False):       
                                               
    timestamp = get_timestamp_from_filename(filename, 
                                            filename_pattern)
                                            
    new_path_str = create_path_str_from_timestamp(timestamp, 
                                                  folder_struct_root_dir, 
                                                  folder_struct)
    # Create the new directories if they do not yet exist
    if not path.exists(new_path_str):
        logging.debug('Creating new dir: ' + new_path_str)
        makedirs(new_path_str)
    # If there is already a copy of the file there...
    if path.exists(path.join(new_path_str,filename)):
        logging.debug('%s is already at destination directory...', filename)
        # ... overwrite it        
        if overwrite==True:
            logging.debug(' ...overwriting file in destination dir')
            shutil.move(path.join(file_dir,filename), new_path_str)
        # ... or just remove original file
        elif erase_if_exists==True:
            logging.debug(' ...erasing it')
            os.remove(path.join(file_dir,filename))
    else:
            logging.debug('Moving %s to %s', filename,new_path_str)
            shutil.move(path.join(file_dir,filename), new_path_str)
    
def get_timestamp_from_filename(filename, pattern):
    timestamp = datetime.strptime(filename, pattern)
    return timestamp
    
def create_path_str_from_timestamp(timestamp, root_path, path_struct):
    path_str = path.join(root_path, timestamp.strftime(path_struct))
    return path_str

def gzip_file(filename, delete_source_file=False):
    import gzip
    import os
    gz_filename = filename + '.gz'
    f_in = open(filename, 'rb')
    f_out = gzip.open(gz_filename, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    if delete_source_file == True:
        os.remove(filename)
    return gz_filename

def listdir_fullpath(directory):
    import os
    return [os.path.join(directory, f) for f in os.listdir(directory)]

def copy_file_via_scp(filename,
                      user='chwala-c', 
                      server='procema', 
                      remotepath='BZB_data_test', 
                      move_to_refug=True,
                      check_refug=True,
                      refug_dir='REFUGIUM',
                      move_to_archive=True,
                      archive_dir='ARCHIVE'):
    from subprocess import Popen    
    from shutil import move
    from fnmatch import fnmatch
    
    # Copy file via scp
    returncode = Popen(["scp", 
                        "-oConnectTimeout=2", 
                        "-oConnectionAttempts=2", 
                        "-oPasswordAuthentication=no", 
                        "-oKbdInteractiveAuthentication=no", 
                        "-oChallengeResponseAuthentication=no", 
                        filename, 
                        "%(user)s@%(server)s:%(remotepath)s" % vars()]).wait() 
    # Check if scp copy workd
    if returncode==0:
        print 'SSH transfer of ' + filename + ' complete'
        if move_to_archive==True:
            move(filename, archive_dir)
            print 'moved ' + filename + 'to ARCHIVE ' + archive_dir
        
        # Check REFUGIUM for files and copy them
        if check_refug == True:
            print 'Checking REFUGIUM'
            for f in listdir_fullpath(refug_dir):
                if fnmatch(f, '*.dat.gz'):
                    print 'Moving ' + f + ' from REFUGIUM to server'
                    copy_file_via_scp(f, 
                                      user=user, 
                                      server=server, 
                                      remotepath=remotepath,
                                      move_to_refug=False, 
                                      check_refug=False,
                                      move_to_archive=True,
                                      archive_dir=archive_dir)                    
    # If scp copy did not work, move files to REFUGIUM                                      
    else:
        print 'SSH transfer of ' + filename + ' failed'
        if move_to_refug == True:
            move(filename, refug_dir)
            print 'file moved to REFUGIUM'   

     
class SessionTimer():
    '''Timer to trigger SNMP queries and streaming to file'''
    def __init__(self,
                 trigger_wait_sec = 5,
                 new_file_wait_minutes = 1,
                 SnmpDAQSessions = None,
                 new_file_trigger_queue = None
                 ):
        self.trigger_wait_sec = trigger_wait_sec
        self.new_file_wait_minutes = new_file_wait_minutes
        self.SnmpDAQSessions = SnmpDAQSessions
        self.trigger_sleep_event = Event()
        self.file_sleep_event = Event()
        self.trigger_timer_loop_process = Process(
            target=self.trigger_timer_loop)
            #target=self.timer, args=(1, ))
        self.file_timer_loop_process = Process(target=self.file_timer_loop)
        self.data_file = None
        self.new_file_trigger_queue = new_file_trigger_queue

    def start(self):
        self.trigger_timer_loop_process.start()
        self.file_timer_loop_process.start()

    def stop(self):
        self.trigger_timer_loop_process.terminate()
        self.file_timer_loop_process.terminate()
        self.trigger_timer_loop_process.join()
        self.file_timer_loop_process.join()
            
    def smart_sleep(self, n_sec=None, n_min=None):
        """ Sleep till next multiple of n_sec or n_min is reached
        
            Only one of the arguments can be supplied and the function only 
            works correctly with n_sec % 60 == 0 and n_min % 60 == 0. Otherwise 
            there will be jumps at the full minute and hour, respectivliy         
        """
    
        from time import sleep
        from datetime import datetime, timedelta
        t = datetime.utcnow()
        
        if n_sec != None and n_min == None:
            t_usec = t.second*1e6 + t.microsecond
            t_wait_usec = n_sec*1e6 - (int(t_usec) % int(n_sec*1e6))
            #t_goal_usec = t_usec + t_wait_usec
            t_goal = t + timedelta(t_wait_usec/24.0/60/60/1e6)
    
        elif n_min != None and n_sec == None:
            t_usec = t.minute*60*1e6 + t.second*1e6 + t.microsecond
            t_wait_usec = n_min*60*1e6 - (int(t_usec) % int(n_min*60*1e6))
            #t_goal_usec = t_usec + t_wait_usec
            t_goal = t + timedelta(t_wait_usec/24.0/60/60/1e6)
        
        else:
            raise(ValueError('one of the arguments n_sec or n_min must \
                              be None or must not be supplied'))
        
        t_wait_sec = t_wait_usec/1e6
        sleep((t_wait_sec))
        t_reached = datetime.utcnow()
        #print t
        #print t_goal
        #print t_reached
        #print (t_reached - t_goal).total_seconds()
        while t_reached < t_goal:
            t_diff_sec = (t_goal - t_reached).total_seconds()
            sleep(t_diff_sec)
            t_reached = datetime.utcnow()
            #print 'extra sleep of ' + str(t_diff_sec)
            #print t_reached

    def trigger_timer_loop(self):
        # Ignore SIGINT to be able to handle it in main() 
        # and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.debug('Started Timer loop with PID %d', getpid())
        while True:
            logging.debug('------------ TRIGGER --------------')
            for session in self.SnmpDAQSessions:
                # check that the query process has already finished
                if session._is_idle == True:
                    #logging.debug(' trigger!')
                    session.trigger_queue.put('TRIGGER')
                else:
                    logging.warning('SessionTimer has detected a timeout of ' +
                                    'SNMP request at MwLinkSite with IP' 
                                    + session.IP)
            self.smart_sleep(self.trigger_wait_sec)

    def file_timer_loop(self):
        # Ignore SIGINT to be able to handle it in main() 
        # and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        while True:
            logging.debug('-------- NEW FILE TRIGGER ----------')
            # wait for the next full minute and trigger writing to file
            self.smart_sleep(n_min=1)
            logging.debug('open new file at ' 
                          + datetime.utcnow().strftime('%Y%m%d_%H%M%S'))
            self.new_file_trigger_queue.put('WRITE_TO_FILE')
            # sleep 0.1 second to avoid starting the smart_sleep to early
            # which may cause it not to sleep            
            #sleep(0.1)
