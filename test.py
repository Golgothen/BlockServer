from connection import Connection
from message import Message
from mplogger import *
from multiprocessing import Queue

if __name__ == '__main__':
    loggingQueue = Queue()
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(sender_config)
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue

    logging.config.dictConfig(sender_config)
    logger = logging.getLogger('test')
    
    c = Connection(config)
    c.connect('localhost',2345)
    c.send(Message('GET_DATA'))
    g = c.recv()
    
