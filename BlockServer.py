from mplogger import *
from game import *
import multiprocessing, pickle
from time import sleep
from datetime import datetime
from job import Job



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
    t = datetime.now()
    print('Creating first job')
    j = Job(game = g, config = config, pick_size = 8)
    print('Pulling {}'.format(j.get()))
    print('Saving first job')
    with open('Lotto8.dat','wb') as f:
        pickle.dump(j, f)
    del j
    
    print('Loading first job')
    with open('Lotto8.dat','rb') as f:
        j = pickle.load(f)
    j.setLogger(config)
    
    print('Pulling {}'.format(j.get()))
    print('Took {}'.format(datetime.now() - t))
    j.purge()

    
    
    
    #bl, b, m = j.get()
    #logger.debug('Received block {}'.format(bl))
    #b.divisions = vector([0,1,0,1,2,5])
    #m.divisions = vector([0,1,4,8,12,15])
    #j.submit(bl, b, m)
    #j.purge()
    listener.stop()