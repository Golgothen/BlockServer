from mplogger import *
from game import *
import multiprocessing, socket
from time import sleep
from datetime import datetime
from job import Job
from client import Client


if __name__ == '__main__':
    loggingQueue = multiprocessing.Queue()
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(sender_config)
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue

    logging.config.dictConfig(sender_config)
    logger = logging.getLogger('application')
    
    g = Lotto()
    g.load('lotto.csv')
    
    

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('',2345))
    try:
        server.listen(5)
        while True:
            client = Client(config, server.accept(), g)
            client.start()
    except (KeyboardInterrupt, SystemExit):
        print('Exiting')
        
        
    
    
    
    listener.stop()