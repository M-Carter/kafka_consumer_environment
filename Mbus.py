#!/usr/bin/python
#Matthew Carter 2017

import kafka, atexit, six, sys, logging
from copy import deepcopy

log = logging.getLogger(__name__)

class Mbus:

    def __init__(self, **kwargs):
        
        #global configuration dictionary
        g_configs = {}
        
        required_args = ('bootstrap_servers', 'topic')
        assert all(val in six.iterkeys(kwargs) for val in (required_args)) and isinstance(kwargs, dict), (
                '{0} REQUIRED'.format(required_args))
                
        assert isinstance(kwargs['topic'], str), 'Mbus is intended to attach to a single topic, must be of type <str>'
        self.topic = kwargs['topic']
        
        assert isinstance(kwargs['bootstrap_servers'], list) and kwargs['bootstrap_servers'] is not None, "bootstrap_servers must be of type <list>"
        g_configs['bootstrap_servers'] = kwargs['bootstrap_servers']
        
        if 'ssl_configs' in kwargs and kwargs['ssl_configs']:
            #ssl requires python 2.7.9, Raise exception if version is not greater than 2.7.9
            if sys.version_info < (2,7,9):
                log.warning('kafka needs certain SSL libraries that come standard with 2.7.9 or greater')
                if 'ignore_version' in kwargs and kwargs['ignore_version']:
                    log.critical('exiting, improper python version {0}, , to ignore this warning, set ignore_version=True'.format(sys.version_info))
                    raise Exception('python 2.7.9 or greater is required')
            
            #make sure required arguments are provided if ssl_configs is declared
            required_ssl_arguments = ('ssl_cafile', 'ssl_certfile', 'ssl_keyfile', 'ssl_password', 'security_protocol', 'ssl_check_hostname')
            assert all(val in six.iterkeys(kwargs['ssl_configs']) for val in (required_ssl_arguments)) and isinstance(kwargs['ssl_configs'], dict), (
                '{0} REQUIRED'.format(required_ssl_arguments))
             
            g_configs.update(kwargs['ssl_configs'])
            
        #assign global configuration kwarg to class object
        self.g_configs = g_configs
        
        #placeholders for producer and consumer objects
        self.producer = None
        self.consumer = None
        
        #register close method when init is closed
        assert atexit.register(self.close), 'cannot atexit register close function, cannot continue'
            
        
    def read(self, group_id=None, from_beginning=False):
        
        if not self.consumer:
            r_configs = deepcopy(self.g_configs)
        
            if group_id:
                r_configs['group_id'] = group_id
                
            r_configs['auto_offset_reset'] = 'earliest'
            log.debug('read method configuration: {0}'.format(r_configs))
            self.consumer = kafka.KafkaConsumer(self.topic, **r_configs)
            
        if from_beginning:
            #overwrite group if passed in with from_beginning call
            log.debug('from_beginning called, polling kafka, seeking to earliest offset')
            self.consumer.poll()
            self.consumer.seek_to_beginning()
        
        for message in self.consumer:
            log.debug('consumed offset {0} from topic {1}'.format(message.offset, self.topic))
            return message.value
        
            
    def write(self, message, topic=None, sync=False):
        
        #allow caller to write to any topic, if topic is passed it, it's used, else use the default
        if topic:
            my_topic = topic
        else:
            my_topic = self.topic
        
        #only open the object once
        if not self.producer:
            w_configs = deepcopy(self.g_configs)
            log.debug('write method configuration: {0}'.format(w_configs))
            self.producer = kafka.KafkaProducer(**w_configs)
        
        self.producer.send(my_topic, str(message))
        
        if sync:
            self.producer.flush()
            
            
    def close(self):
    
        '''
        close both consumer and producer, if they exist.
        '''

        if self.producer:
            log.info('closing producer...')
            self.producer.close()
            self.producer = None
            log.info('producer closed')
        
        if self.consumer:
            log.info('closing consumer...')
            self.consumer.close()
            self.consumer = None
            log.info('consumer closed')