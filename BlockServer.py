from mplogger import *
from game import *
import multiprocessing, socket, pickle, os
from time import sleep
from datetime import datetime
from job import Job
from client import Client
from threading import Thread
from copy import deepcopy

LISTENING_PORT = 2345
NEXT_JOB_THRESHOLD = 20
CHECKPOINT_SAVE_MINUTES = 5

if __name__ == '__main__':
    running = True
    def listenerThread():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('',LISTENING_PORT))
        server.listen(50)
        while running:
            client = Client(config, server.accept(), jobs)
            client.start()

    
    loggingQueue = multiprocessing.Queue()
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(sender_config)
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue

    logging.config.dictConfig(sender_config)
    logger = logging.getLogger('application')
    
    jobs = {}
    success = False
    if os.path.isfile('server.dat'):
        try:
            with open('server.dat','rb') as f:
                jobs = pickle.load(f)
                success = True
        except pickle.UnpicklingError:
            logger.error('Error loading server.dat.', exc_info = True)
            success = False
        if success:
            os.remove('server.dat')
            for k, v in jobs.items():
                v.setLogger(config)
    if not success:
        if os.path.isfile('checkpoint.dat'):
            try:
                with open('checkpoint.dat','rb') as f:
                    jobs = pickle.load(f)
                    success = True
            except pickle.UnpicklingError:
                logger.error('Error loading checkpoint.dat.', exc_info = True)
                success = False
            if success:
                os.remove('checkpoint.dat')
                for k, v in jobs.items():
                    v.setLogger(config)
    if not success:
        g = Lotto()
        g.load('lotto.csv')
        
        for i in range(g.minPick, g.maxPick+1):
            jobs['{}{}'.format(type(g).__name__, i)] = Job(config = config, game = g, pick_size = i)

    sp = Thread(target = listenerThread)
    sp.start()
    passCount = 0
    try:
        while True:
            for j, v in jobs.items():
                if not v.isAvailable:
                    v.recycle()
                if v.isActive:
                    print('Job: {:<10} Progress: {:7.3f}%. {:15,.0f} blocks remaining. ({:15,.0f} queued, {:15,.0f} allocated)'.format(j, v.progressPercent, v.blocksRemaining, v.blocksQueued, v.blocksAllocated))
                workAvailable = False
                for k, w in jobs.items():
                    if k == j:
                        continue
                    if w.isActive and w.progressPercent < NEXT_JOB_THRESHOLD:
                        workAvailable = True
                        break
            if not workAvailable:
                for k, v in jobs.items():
                    if v.totalBlocks == 0:
                        v.prep()
                        break
            sleep(30)
            
    except (KeyboardInterrupt, SystemExit):
        running = False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost',LISTENING_PORT))
            s.close()
        except:
            pass
        print('Dumping data...')
        with open('server.dat','wb') as f:
            pickle.dump(jobs, f)
        print('Data dump complete')
    listener.stop()
    
