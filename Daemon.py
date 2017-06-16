#!/usr/bin/python

import sys, os, time, atexit, signal, logging

logger = logging.getLogger(__name__)


class Daemon:

    def __init__(self, pidfile):
        self.pidfile = pidfile

    def create_daemon(self):
        #fork twice so second child process cannot aquire a TTY
        #first fork
        logger.info('starting daemon creation')
        logger.debug('forking parent process')
        pid = os.fork()
        
        if pid == 0:
            logger.debug('now in first child process')
            logger.debug('breaking from parent environment')
            #working in first child process
            #break from parent environment
            os.chdir("/")
            os.setsid()
            os.umask(0)
                
            #second fork
            logger.debug('forking child process')
            pid = os.fork()
        
            if pid == 0:
                logger.debug('now in second child process')
                #working in second child process
                #executes on any exit
                logger.debug('atexit registering self.del_pidfile') 
                atexit.register(self.del_pidfile)

                #create pidfile
                pid = str(os.getpid())
                file(self.pidfile,'w+').write('{0}\n'.format(pid))
                logger.info('pid {0} written to pidfile {1}'.format(pid, self.pidfile))

                #create a rw file descriptor
                logger.debug('creating new bit bucket file descriptor')
                fd = os.open("/dev/null", os.O_RDWR)

                #flush out and err before closing descriptors
                logger.debug('flushing stdout / stderr file descriptors')
                sys.stdout.flush()
                sys.stderr.flush()

                #close open file descriptors
                logger.debug('closing stdin / stdout / stderr file descriptors')
                os.dup2(fd, sys.stdin.fileno())
                os.dup2(fd, sys.stdout.fileno())
                os.dup2(fd, sys.stderr.fileno())
                
                logger.debug('all is done')
                
            else:
                os._exit(0)
                
        else:
            os._exit(0)
  
        
    def get_pid(self):
        try:
            with open(self.pidfile, 'r') as myfile:
                logger.debug('opened pidfile: {0}'.format(self.pidfile))
                pid = int(myfile.read().strip())

        except IOError:
            pid = None
            pass

        logger.debug('pid retrieved: {0}'.format(pid))
        return pid
        

    def del_pidfile(self):
        logger.info('deleting {0}'.format(self.pidfile))
        os.remove(self.pidfile)

        
    def start(self):
        logger.info('starting Daemon')
        pid = self.get_pid()

        if pid:
            raise Exception('Daemon already running, pid {0} from pidfile {1}'.format(pid, self.pidfile))

        self.create_daemon()
        
        return True


    def stop(self):
        pid = self.get_pid()
        if not pid:
            raise IOError('pidfile {0} does not exist, Daemon is not running'.format(self.pidfile))

        logger.info('sending signal SIGKILLto pid {0}'.format(pid))
        os.kill(pid, signal.SIGKILL)
        self.del_pidfile()
        

    def restart(self):
        logger.info('restarting Daemon')
        self.stop()
        self.start()
        

    def status(self):
        pid = self.get_pid()
        procfile = None

        if pid:
            with open('/proc/{0}/status'.format(pid), 'r'):
                procfile = file('/proc/%s/status' % pid, 'r')
            
        return procfile