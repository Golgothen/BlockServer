from multiprocessing import Process, Event, Queue
import logging.handlers, logging.config
from datetime import datetime

sender_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'handlers': {
        'queue': {
            'class': 'logging.handlers.QueueHandler',
            'queue': Queue,
        },
    },
    'loggers': {
        'application': {
            'level':       'DEBUG',
        },
        'job': {
            'level':       'INFO',
        },
        'client': {
            'level':       'INFO',
        },
        'connection': {
            'level':       'INFO',
        },
        'pipewatcher': {
            'level':       'DEBUG',
        },
    },
    'root': {
        'level': 'INFO',
        'handlers': ['queue']
    },
}

listener_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'respect_handler_level': True,
    'formatters': {
        'detailed': {
            'class':       'logging.Formatter',
            'format':      '%(asctime)-16s:%(name)-21s:%(processName)-15s:%(levelname)-8s[%(module)-13s.%(funcName)-20s %(lineno)-5s] %(message)s'
            },
        'brief': {
            'class':       'logging.Formatter',
            'format':      '%(asctime)-16s:%(message)s'
        }
    },
    'handlers': {
        'console': {
            'class':       'logging.StreamHandler',
            'level':       'DEBUG',
            'formatter':   'brief'
        },
        'file': {
            'class':       'logging.FileHandler',
            'filename':    '{}.log'.format(datetime.now().strftime('%Y%m%d')),
            'mode':        'a',
            'formatter':   'detailed',
        },
        'filerotate': {
            'class':       'logging.handlers.TimedRotatingFileHandler',
            'filename':    'server.log',
            'when':        'midnight',
            'interval':    1,
            'formatter':   'detailed',
            'backupCount': 10
        }
    },
    'root': {
        'handlers':    ['console', 'file'],
    },
}


class MyHandler(object):
    def handle(self, record):
        logger = logging.getLogger(record.name)
        logger.handle(record)

class LogListener(Process):
    def __init__(self, logQueue):
        super(LogListener, self).__init__()
        self.__stop_event = Event()
        self.name = 'listener'
        self.logQueue = logQueue

    def run(self):
        logging.config.dictConfig(listener_config)
        listener = logging.handlers.QueueListener(self.logQueue, MyHandler())
        listener.start()
        while True:
            try:
                self.__stop_event.wait()
                listener.stop()
                break
            except (KeyboardInterrupt, SystemExit):
                listener.stop()
                break

    def stop(self):
        self.__stop_event.set()
