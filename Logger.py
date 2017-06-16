#!/usr/bin/python
#Matthew Carter 2017

import logging
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import SysLogHandler
from logging.handlers import RotatingFileHandler
import sys
import os
import inspect
import re

    
class Logger:

    def __init__(self, logger=None, name=__name__, level='INFO'):
        self.logger = logger
        self.name = name
        self.level = level
        self.format = logging.Formatter('%(asctime)s %(module)-12s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')

    def get_logger(self):
        if not self.logger:   
            self.logger = logging.getLogger(self.name)
        return self.logger
        
    def set_level(self, log_level=None):
        self.get_logger()
        
        if log_level:
            self.level = log_level
        
        level = getattr(logging, self.level.upper(), logging.INFO)
        self.logger.setLevel(level)
        return self.logger
        
    def console_handler(self):
        self.set_level()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(self.format)        
        self.logger.addHandler(handler)
        return self.logger
        
    def max_time_handler(self, logfile, time_interval, backup_count = 0):
    
        '''
        this function creates a TimedRotatingFileHandler object.
        
        --logfile is a full path to a file where all logs are to be stored.
        
        --time_interval is the time information when to roll a log over.
            --accepts hour/minute/day arguments and converts to minutes.
                -- i.e 1day, 5hours, 30minutes
                
        --backup_count is how many files of time_interval to keep
        '''


        '''
        test if time_interval is only integers, if so pass as default minutes,
        otherwise try to format hours, days, weeks, to minutes before passing
        '''
        
        if type(time_interval) is not int:
        
            if not time_interval.isdigit():
        
                time_interval = time_interval.upper()
                a = int(re.sub("[^0-9]", "", time_interval))
                b = re.sub("[^A-Z]", "", time_interval)
            
                if b.startswith('M'):
                    time_interval = a * 1
                    
                elif b.startswith('H'):
                    time_interval = a * 60
                
                elif b.startswith('D'):
                    time_interval = a * 60 * 24
                
                elif b.startswith('W'):
                    time_interval = a * 60 * 24 * 7
                    
                else:
                    raise ValueError('invalid format for time_interval: {0}'.format(max_bytes))
        
        #--------------------------------------------------------------------
    
        self.set_level()
        handler = TimedRotatingFileHandler(logfile, when='m', interval=time_interval, backupCount=backup_count)
        handler.setFormatter(self.format)
        self.logger.addHandler(handler)
        return self.logger
        
    def max_size_handler(self, logfile, max_bytes, backup_count = 0, write_mode = 'a'):
    
        '''
        this function creates a RotatingFileHandler object.
        
        --logfile is a full path to a file where all logs are to be stored.
        
        --max_bytes is the maximum size of the file in bytes.
            --accepts file sizing arguments and converts to bytes before creating the object
                -- i.e 1MB, 1GB, etc
                
        --backup_count is how many files of max_bytes to keep
        
        --write_mode by default is 'a' or append.  Write mode can be called to change it's default
          behavior
        '''

        '''
        test if max_bytes is only integers, if so pass as default bytes,
        otherwise try to format MB, GB, TB to bytes before passing
        '''
        
        if type(max_bytes) is not int:
        
            if not max_bytes.isdigit():
            
                max_bytes = max_bytes.upper()
                a = int(re.sub("[^0-9]", "", max_bytes))
                b = re.sub("[^A-Z]", "", max_bytes)
            
                if b.startswith('K'):
                    max_bytes = a * 1024
                    
                elif b.startswith('M'):
                    max_bytes = a * 1024 * 1024
                
                elif b.startswith('G'):
                    max_bytes = a * 1024 * 1024 * 1024
                
                elif b.startswith('T'):
                    max_bytes = a * 1024 * 1024 * 1024 * 1024
                    
                else:
                    raise ValueError('invalid format for max_bytes: {0}'.format(max_bytes))
        
        #--------------------------------------------------------------------
    
    
        self.set_level()
        handler = RotatingFileHandler(logfile, mode = write_mode, maxBytes = max_bytes, backupCount = backup_count)
        handler.setFormatter(self.format)
        self.logger.addHandler(handler)
        return self.logger
        
        
    def syslog_handler(self):
        self.set_level()
        handler = SysLogHandler(address = '/dev/log')
        syslog_format = logging.Formatter('%(name)-12s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
        handler.setFormatter(syslog_format)
        self.logger.addHandler(handler)
        return self.logger
        

        