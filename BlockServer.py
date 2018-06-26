from mplogger import *
from job import Job
from game import *
import multiprocessing



if __name__ == '__main__':
    loggingQueue = multiprocessing.Queue()
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(sender_config)
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue

    logging.config.dictConfig(config)
    logger = logging.getLogger('application')
    
    g = Lotto()
    g.load('lotto.csv')
    j = Job(game = g, config = config)
    bl, b, m = j.get()
    logger.debug('Received block {}'.format(bl))
    b.divisions = vector([0,1,0,1,2,5])
    m.divisions = vector([0,1,4,8,12,15])
    j.submit(bl, b, m)
    j.purge()
    listener.stop()