# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 13:16:15 2014

@author: chwala-c
"""

import netsnmp
import signal
import logging
import errno
import shutil
import os
import gzip

from os import getpid, makedirs, path
from multiprocessing import Event, Process, Queue, Pool
from datetime import datetime
from time import sleep, time
from collections import namedtuple

import numpy as np
import pandas as pd

# Configuration file for SNMP, paths and timers
import config

DATA_FILE_FORMAT_VERSION = '1.0'

# Define messages that will be sent over the different queues
Message = namedtuple('Message', ['type', 'content'])
NEW_FILE_MESSAGE = Message(type='trigger', content='new file')
STOP_MESSAGE = Message(type='control', content='stop')
# Example for a message to start SNMP queries, here for query batch #1
# START_SNMP_QUERIES = Message(type='trigger', content=1)


def pySNMPdaq_loop():
    """
    The main loop for pySNMPdaq that is run till infinity

    This function takes care of all the initialization for the startup
    and all the cleaning up for the termination of pySNMPdaq. It can be
    called directly. Then it is terminated by Ctrl+C. Or it can be
    called as the run method of a daemon. Then it is terminated by SIGTERM.

    It reads the config from the local file config.py and the list of IPs
    and OIDs from mw_link_OID_listing.py.

    """

    print '\n### Starting pySNMPdaq main loop ###'

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
                            mw_link_OID_listing)
    logging.info('Current config file: %s', config_file_name)

    # Separate OID_listings for different timers
    ip_oid_list_per_timer = {}
    for ip_oid_entry in mw_link_OID_listing.mw_link_list:
        timer_n = ip_oid_entry['timer']
        if timer_n not in ip_oid_list_per_timer.keys():
            ip_oid_list_per_timer[timer_n] = []
        ip_oid_list_per_timer[timer_n].append(ip_oid_entry)

    print '\n### Number of request per timer batch ###'
    for i, timer_id in enumerate(ip_oid_list_per_timer.keys()):
        print('Timer ID: %s  N_requests: %d  '
              'triggered_every i*%d+%d seconds ' %
              (timer_id,
               len(ip_oid_list_per_timer[timer_id]),
               config.SNMP_QUERY_MAIN_WAIT_SEC,
               i*config.SNMP_QUERY_BETWEEN_BATCHES_WAIT_SEC)
              )

    # Assert that different query timers do not interfer
    timer_ids = ip_oid_list_per_timer.keys()
    if (config.SNMP_QUERY_BETWEEN_BATCHES_WAIT_SEC * len(timer_ids)
        > config.SNMP_QUERY_MAIN_WAIT_SEC):
        raise ValueError('The temporal offset between the SNMP request '
                         'batches will interfere with the main wait time '
                         'between the requests, i.e. between request i '
                         'of batch#1 and request i+1 of batch#1.')

    # Init timers for SNMP queries
    query_trigger_queue = Queue()
    query_timers = []
    for i_timer, timer_id in enumerate(timer_ids):
        query_timers.append(
            Timer(n_sec=config.SNMP_QUERY_MAIN_WAIT_SEC,
                  offset_sec=config.SNMP_QUERY_BETWEEN_BATCHES_WAIT_SEC*i_timer,
                  target_queue=query_trigger_queue,
                  message=Message(type='trigger', content=timer_id),
                  print_debug_info=False))

    # Init DataHandler and its Timer that triggers the creation of a new file
    query_results_queue = Queue()
    new_file_trigger_queue = Queue()

    # new_file_timer = Timer(n_sec=config.NEW_FILE_WAIT_SEC,
    #                       target_queue=new_file_trigger_queue,
    #                       message=NEW_FILE_MESSAGE,
    #                       print_debug_info=False)

    dataHandler = DataHandler(query_results_queue=query_results_queue,
                              new_file_trigger_queue=new_file_trigger_queue,
                              data_dir=config.DATA_DIR,
                              filename_prefix=config.FILENAME_PREFIX,
                              date_format=config.DATE_FORMAT,
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

    # Start timers
    # new_file_timer.start()
    for query_timer in query_timers:
        query_timer.start()

    # Define signal handler for SIGTERM which is sent to stop loop
    # when running as daemon.
    signal.signal(signal.SIGTERM, sigterm_handler)

    # Initialize process pool
    proc_pool = Pool(500)

    # Start infinite loop. It can be exited by raising SystemExit,
    # which is done in the signal handler. The 'finally' part than
    # takes care of stopping all the processes and clean up.
    try:
        while True:
            # Wait for trigger from timers
            message = query_trigger_queue.get()
            timer_id = message.content

            dataHandler.new_file_trigger_queue.put(NEW_FILE_MESSAGE)

            # Start the parallel SNMP queries and
            # put results in queue for dataHandler
            proc_pool.map_async(snmp_query,
                                ip_oid_list_per_timer[timer_id],
                                callback=query_results_queue.put)

    except KeyboardInterrupt:
        print 'main() received KeyboardInterrupt'

        print 'Exiting timers'
        # new_file_timer.stop()
        for query_timer in query_timers:
            query_timer.stop()

        print 'Stopping process pool'
        proc_pool.close()
        proc_pool.join()

        print 'Exiting data handler'
        dataHandler.stop()

    finally:
        logging.debug('Daemon is trying to clean up!')
        logging.debug('Trying to stop timer processes...')
        # new_file_timer.stop()
        for query_timer in query_timers:
            query_timer.stop()

        logging.debug('Trying to stop dataHandler processes...')
        dataHandler.stop()
        sleep(0.1)

        logging.debug('Stopping process pool...')
        proc_pool.close()
        proc_pool.join()

        logging.debug('Clean up done. Exit!')
        print 'Exit!'

    return
 
 
def sigterm_handler(signal, frame):
    """ Handler function to catch SIGTERM and trigger clean exit """
    logging.debug('pySNMPdaq got SIGTERM')
    import sys
    # Raise SystemExit. That causes the main loop to switch to
    # the 'finally' after the 'try'
    sys.exit(0)


def make_sure_path_exists(path, do_logging=True):
    """ Create a path if it does not exist """
    try:
        makedirs(path)
        if do_logging:
            logging.debug('Created %s', path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            logging.debug('Creation of %s went wrong', path)
            raise
        else:
            if do_logging:
                logging.debug('Path %s already existed', path)


def save_timestamped_config_and_mw_links_list(mw_link_OID_listing):
    """ Save pySNMPdaq config and MW link OID listing in a unified file """
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
    """ Build a numpy record array to store MW link query """
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
    """ Build a nicely formated string for a MW link record """
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
    """ Reorder columns of a Pandas DataFrame """
    columns = df.columns.tolist()
    ordered_columns = []
    for item in order:
        columns.remove(item)
        ordered_columns.append(item)
    ordered_columns += columns
    return df[ordered_columns]


def snmp_query(ip_oid_dict):
    # Define signal handler for SIGTERM which is sent to stop loop
    # when running as daemon.
    signal.signal(signal.SIGTERM, sigterm_handler)

    ID = ip_oid_dict['ID']
    IP = ip_oid_dict['IP']
    oid_dict = ip_oid_dict['OID_dict']

    # FOR DEBUGGING
    # print 'Query for %s ID at %s' % (ID, str(datetime.utcnow()))

    mw_link_record = build_empty_mw_link_record_array(oid_dict)

    var_list = netsnmp.VarList()
    for oid in oid_dict.values():
        logging.debug(' for IP ' + str(IP) + ' appending to var_list: ' + oid)
        var_list.append(netsnmp.Varbind(oid))

    # Init SNMP Session for different protocol versions
    if config.SNMP_VERSION == 1 or config.SNMP_VERSION == 2:
        logging.debug(' open session with SNMPv1 for ' + IP)
        SnmpSession = netsnmp.Session(DestHost=IP,
                                      Version=config.SNMP_VERSION,
                                      Community=config.SNMP_COMMUNITY,
                                      Timeout=int(config.SNMP_TIMEOUT_SEC*1000000),
                                      Retries=config.SNMP_RETRIES)
    elif config.SNMP_VERSION == 3:
        logging.debug(' open session with SNMPv3 for ' + IP)
        SnmpSession = netsnmp.Session(DestHost=IP,
                                      Version=config.SNMP_VERSION,
                                      SecName=config.SNMP_USERNAME,
                                      AuthPass=config.SNMP_AUTHPASSWORD,
                                      AuthProto='MD5',
                                      PrivProto='DES',
                                      SecLevel='authNoPriv',
                                      Timeout=int(config.SNMP_TIMEOUT_SEC*1000000),
                                      Retries=config.SNMP_RETRIES)
    else:
        raise ValueError('SNMP_VERSION must be 1, 2 or 3.')

    # Start time for round trip time
    t1 = time()
    # Send SNMP query
    query_result = SnmpSession.get(var_list)
    # Get round trip time (RTT)
    query_rtt = time() - t1
    # Put RTT and a timestamp in mw_link_record
    mw_link_record['Timestamp_UTC'] = np.datetime64(datetime.utcnow(), 'us')
    mw_link_record['RTT'] = query_rtt
    mw_link_record['MW_link_ID'] = ID

    # Write SNMP query to mw_link_record according to OIDs
    for i, oid_name in enumerate(oid_dict.keys()):
        # print self.IP, self.oid_name_list, self.query_result, i, self.query_result[i]
        # print self.IP, self.mw_link_record
        if query_result[i] is not None:
            mw_link_record[oid_name] = query_result[i]
        else:
            mw_link_record[oid_name] = np.NaN

    # Put data into queue for dataHandler process
    # results_queue.put(mw_link_record)

    #
    # WORKAROUND FOR PROBLEM WITH REPEATED QUERIES OF
    # Tx-, Rx-, etc. data --> renew varlist
    #
    # IS THIS STILL NECESSARY WHEN USING OTHER MW LINKS ???!!
    #
    var_list = netsnmp.VarList()
    for oid in oid_dict.values():
        # logging.debug(' for IP ' + str(self.mwLinkSite.IPaddress) + ' appending to var_list: ' + oid)
        var_list.append(netsnmp.Varbind(oid))

    # Tell everybody that we are idle now
    # self._is_idle = True

    # logging.debug(' idle')
    # print self.IP +  ' IDLE'

    return mw_link_record

            
class DataHandler:
    """Data handler which will be looped in a seperate process to manage
       the data streams from the SNMP queries"""
    def __init__(self,
                 query_results_queue=None,
                 new_file_trigger_queue=None,
                 data_dir='data',
                 filename_prefix='mw_link_queries',
                 date_format='%Y%m%d_%H%M%S',
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
        self.date_format = date_format
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

        self.KEEP_RUNNING = True
        self.NEW_DataFrame = Event()
        self.WRITE_TO_FILE = Event()               

        self.df = pd.DataFrame()

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
        while self.KEEP_RUNNING:
            logging.debug('_listener_loop is waiting for message')
            message = self.new_file_trigger_queue.get()
            logging.debug('_listener_loop got message %s', message)
            if message == STOP_MESSAGE:
                break
            if message == NEW_FILE_MESSAGE:
                self.WRITE_TO_FILE.set()
                # give the data handler some time to write 
                # DataFrame to file and to initialize new DataFrame
                sleep(0.1)
                # then check if the WRITE_TO_FILE is cleared, 
                # that is, the file handler has opend new file
                if self.WRITE_TO_FILE.is_set():
                    logging.warning('File handler did not clear WRITE_TO_FILE Event')
        logging.debug('Exit DataHandler._listener_loop')

    def _data_handler_loop(self):
        # Ignore SIGINT to be able to handle it in 
        # main() and close processes cleanly
        self.record = None
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.debug('Started DataHandler data_handler loop with PID %d', getpid())
        while self.KEEP_RUNNING:
            sleep(0.01)
            if self.WRITE_TO_FILE.is_set():
                # write to file, call for new DataFrame,
                # close old file and open new one
                self._write_mw_link_record_to_file()
                self.NEW_DataFrame.set()
                self._close_file()
                self._open_new_file()
                
                self.WRITE_TO_FILE.clear()

            # get data from queue and write to DataFrame which will
            # be writen to file when WRITE_TO_FILE Event is set
            while not self.query_results_queue.empty():
                # Step out of this loop if writing to file is in progress
                if self.WRITE_TO_FILE.is_set():
                    break

                # Get data from queue
                queue_item = self.query_results_queue.get()
                # print 'queue_item ', queue_item
                if queue_item == STOP_MESSAGE:
                    break
                if type(queue_item) == list:
                    record_list = queue_item
                else:
                    record_list = [queue_item, ]

                if self.NEW_DataFrame.is_set():
                    self.df = pd.DataFrame()
                    # self.df.set_index('Timestamp_UTC', inplace=True)
                    self.NEW_DataFrame.clear()
                    logging.debug('----- NEW DataFrame ----')

                for record in record_list:
                    df_temp = pd.DataFrame(record)
                    df_temp.set_index('Timestamp_UTC', inplace=True)
                    self.df = self.df.append(df_temp)
                    self.df = reorder_columns_of_DataFrame(self.df)

                # print ''
                # print self.df.sort_index()

            # if self.record == 'EXIT':
            #    break

        logging.debug('Exit DataHandler._data_handler_loop')

    def _ssh_loop(self):
        from os import path
        # Ignore SIGINT to be able to handle it in 
        # main() and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.debug('Started DataHandler ssh_loop with PID %d', getpid())
        while self.KEEP_RUNNING:
            message = self.ssh_filename_queue.get()
            logging.debug('_ssh_loop got message %s', message)

            if message == STOP_MESSAGE:
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
                             + datetime.utcnow().strftime(self.date_format)
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
        if self.ssh_transfer:
            self.ssh_filename_queue.put(gz_filename)
            
        # copy file to data outbox directory
        if self.put_data_to_out_dir:
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
        if self.archive_files:
            try:
                fn_pattern = (self.filename_prefix
                              + '_'
                              + self.date_format
                              + '.dat.gz')
                move_timestamped_file_to_folder_struct(
                    filename=path.split(gz_filename)[1],
                    file_dir=path.split(gz_filename)[0],
                    filename_pattern=fn_pattern,
                    folder_struct_root_dir=path.join(self.data_dir, 
                                                     self.archive_dir))
            except:
                logging.warn('Could not move file to folder structure')
        
        # or do nothing        
        else:
            pass

    def _write_mw_link_record_to_file(self):
        # Write data to file if the DataFrame is
        # not the empty one which was initialized
        # in __init__.
        if not self.df.empty:
            self.df.sort_index().to_csv(self.data_file,
                                        na_rep='NaN',
                                        float_format='%.3f')

    def _move_files_to_archive(self):
        pass

    def start(self):
        self.listener_loop_process.start()
        self.data_handler_loop_process.start()
        self.ssh_loop_process.start()

    def stop(self):
        # logging.debug('DataHandler.stop: clearing KEEP_RUNNIG')
        # self.KEEP_RUNNING = False
        # sleep(0.1)
        
        # Write out last data to file
        try:
            self._write_mw_link_record_to_file()
        except:
            logging.warning('DataHandler.stop: could not write file')
        try:
            self._close_file()
        except:
            logging.warning('DataHandler.stop: could not close file')

        # Send STOP message via queues and terminate processes
        logging.debug('DataHandler.stop: sending STOP_MESSAGE')
        self.ssh_filename_queue.put(STOP_MESSAGE)
        self.new_file_trigger_queue.put(STOP_MESSAGE)
        self.query_results_queue.put(STOP_MESSAGE)
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
    if path.exists(path.join(new_path_str, filename)):
        logging.debug('%s is already at destination directory...', filename)
        # ... overwrite it        
        if overwrite:
            logging.debug(' ...overwriting file in destination dir')
            shutil.move(path.join(file_dir, filename), new_path_str)
        # ... or just remove original file
        elif erase_if_exists:
            logging.debug(' ...erasing it')
            os.remove(path.join(file_dir, filename))
    else:
            logging.debug('Moving %s to %s', filename, new_path_str)
            shutil.move(path.join(file_dir, filename), new_path_str)


def get_timestamp_from_filename(filename, pattern):
    timestamp = datetime.strptime(filename, pattern)
    return timestamp


def create_path_str_from_timestamp(timestamp, root_path, path_struct):
    path_str = path.join(root_path, timestamp.strftime(path_struct))
    return path_str


def gzip_file(filename, delete_source_file=False):
    gz_filename = filename + '.gz'
    f_in = open(filename, 'rb')
    f_out = gzip.open(gz_filename, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    if delete_source_file:
        os.remove(filename)
    return gz_filename


def listdir_fullpath(directory):
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
        if move_to_refug:
            move(filename, refug_dir)
            print 'file moved to REFUGIUM'   


class Timer:
    def __init__(self,target_queue, message, n_sec=None, n_min=None, offset_sec=0, print_debug_info=False):
        self.target_queue = target_queue
        self.message = message
        self.n_sec = n_sec
        self.n_min = n_min
        self.offset_sec = offset_sec
        self.print_debug_info = print_debug_info

        self._keep_running = True

        self._timer_loop_process = Process(target=self._timer_loop)

    def _timer_loop(self):
        # Ignore SIGINT to be able to handle it in main()
        # and close processes cleanly
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        while self._keep_running:
            smart_sleep(n_sec=self.n_sec,
                        n_min=self.n_min,
                        offset_sec=self.offset_sec,
                        print_debug_info=self.print_debug_info)
            self.target_queue.put(self.message)
            if self.print_debug_info:
                print 'Timer sent message %s' % str(self.message)
            logging.debug('Timer sent message %s' % str(self.message))
        logging.debug('Stopping timer who sent message %s' % str(self.message))

    def start(self):
        self._timer_loop_process.start()

    def stop(self):
        self._keep_running = False
        logging.info('Stopping timer with message %s. This may take some time' % str(self.message))
        if self.n_sec is None:
            print 'Stopping timer. This may take up to %d minutes' % self.n_min
        elif self.n_min is None:
            print 'Stopping timer. This may take up to %d seconds' % self.n_sec
        else:
            print 'n_min or n_sec must not be None'
        self._timer_loop_process.terminate()
        self._timer_loop_process.join()
        print 'Timer stopped'


def smart_sleep(n_sec=None, n_min=None, offset_sec=0, print_debug_info=False):
    """ Sleep till next multiple of n_sec or n_min is reached

        Only one of the arguments can be supplied and the function only
        works correctly with n_sec % 60 == 0 and n_min % 60 == 0. Otherwise
        there will be jumps at the full minute and hour, respectivliy
    """

    # Ignore SIGINT to be able to handle it in main()
    # and close processes cleanly
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    from time import sleep
    from datetime import datetime, timedelta
    t = datetime.utcnow()

    if n_sec is not None and n_min is None:
        t_usec = t.second * 1e6 + t.microsecond
        t_wait_usec = n_sec * 1e6 - (int(t_usec) % int(n_sec * 1e6))

    elif n_min is not None and n_sec is None:
        t_usec = t.minute * 60 * 1e6 + t.second * 1e6 + t.microsecond
        t_wait_usec = n_min * 60 * 1e6 - (int(t_usec) % int(n_min * 60 * 1e6))

    else:
        raise (ValueError('one of the arguments n_sec or n_min must \
                           be None or must not be supplied'))

    t_wait_sec = t_wait_usec / 1e6
    t_wait_sec += offset_sec
    t_goal = t + timedelta(t_wait_sec / 24.0 / 60 / 60)

    sleep(t_wait_sec)
    t_reached = datetime.utcnow()

    if print_debug_info:
        print 't                  = ', t
        print 't_goal             = ', t_goal

        print 't_reached          = ', t_reached
        print 't_reached - t_goal = %d seconds' % (t_reached - t_goal).total_seconds()

    while t_reached < t_goal:
        t_diff_sec = (t_goal - t_reached).total_seconds()
        sleep(t_diff_sec)
        t_reached = datetime.utcnow()
        if print_debug_info:
            print '  extra sleep of ' + str(t_diff_sec)
            print '  t_reached = ', t_reached
