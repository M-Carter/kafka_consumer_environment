#!/usr/bin/python

#consumer template

#python libs
import six
import inspect
import os
import argparse
import sys
import logging

from Logger import Logger
from Daemon import Daemon
from Mbus2 import Mbus
    
    
caller = os.path.basename(inspect.stack()[1][1])
logfile = '/var/log/{0}.log'.format(caller)
pidfile = '/var/run/{0}.pid'.format(caller)

    
def create_consumer_environment():

    try:
        action = str(sys.argv[1])
        sys.argv.pop(1)
    
    except IndexError as e:
        err_msg = 'missing arg <start/stop/restart/status>'
        print ('\n\n{0}\n\n'.format(err_msg))
        raise(e)
    
    if action == 'start':
        return argparse_set_consumer_environment()
    elif action == 'stop':
        print daemon_action(pidfile, 'stop')
        sys.exit(0)
    elif action == 'restart':
        print daemon_action(pidfile, 'restart')
        sys.exit(0)
    elif action == 'status':
        print daemon_action(pidfile, 'status')
        sys.exit(0)
    
def argparse_set_consumer_environment():
    configs = {
                'logfile': logfile,
                'pidfile': pidfile
            }
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', required=True, nargs='+', help='kafka servers to attach')
    parser.add_argument('--topic', action='store', required=True, type=str, help='kafka topic to subscribe to')
    parser.add_argument('--level', type=str, choices=['debug', 'info', 'warning', 'critical'], default='INFO', help="logging level")
    parser.add_argument('-c', '--console', action="store_true", default=False, help='run everything in the console, do not background')
    
    opts = parser.parse_args()
    _dict = vars(opts)
    
    configs.update(_dict)
    
    return set_consumer_environment(**configs)
    
    
def set_consumer_environment(**kwargs):
    '''
    take a logging instance, log level, and a logfile to attach the appropriate logging handlers
    '''

    required_args = ('level', 'logfile', 'console', 'bootstrap_servers', 'topic')
    assert all(val in six.iterkeys(kwargs) for val in (required_args)) and isinstance(kwargs, dict), (
                '{0} REQUIRED'.format(required_args))
                
    #init            
    logger = None
    daemon = None
    mbus = None

    logger = _logger(**kwargs)
    
    if not kwargs['console']:
        daemon = _daemon(kwargs['pidfile'])
        daemon.start()
        
        
    logger.info('creating message bus handler...{0}'.format(kwargs['bootstrap_servers']))
    mbus = _mbus(**kwargs)
    
    return logger, daemon, mbus
    

def _logger(**kwargs):
    logging_base = logging.getLogger()
    logger_init = Logger(logging_base, level=kwargs['level'])
    
    if kwargs['console']:
        logger = logger_init.console_handler()
        logger.warning('CONSOLE mode ENABLED, output will not be logged!')
    
    else:
        logger = logger_init.max_time_handler(kwargs['logfile'], '1day', 5)
        logger.info('added max time file handler: {0}'.format(kwargs['logfile']))
        
    return logger
    
    
def daemon_action(pidfile, action):
    daemon = _daemon(pidfile)
    daemon_method = getattr(daemon, action)
    
    return daemon_method()


def _daemon(pidfile):
    daemon = Daemon(pidfile)
    
    return daemon


def _mbus(**kwargs):
    mbus = Mbus(**kwargs)
    
    return mbus

        
