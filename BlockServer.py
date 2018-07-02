from mplogger import *
from game import *
import multiprocessing, socket, pickle, os
from time import sleep
from datetime import datetime
from job import Job
from client import Client
from threading import Thread

LISTENING_PORT = 2345
    

if __name__ == '__main__':
    running = True
    def listenerThread():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('',LISTENING_PORT))
        server.listen(5)
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
    if os.path.isfile('server.dat'):
        with open('server.dat','rb') as f:
            jobs = pickle.load(f)
        os.remove('server.dat')
        for k, v in jobs.items():
            v.setLogger(config)
    else:
        g = Lotto()
        g.load('lotto.csv')
        
        for i in range(g.minPick, g.maxPick+1):
            jobs['{}{}'.format(type(g).__name__, i)] = Job(config = config, game = g, pick_size = i)
        
        jobs['{}{}'.format(type(g).__name__, g.minPick)].prep()

    #for k, v in jobs.items():
    #    print(k, v.pickSize)
    
    sp = Thread(target = listenerThread)
    sp.start()
    try:
        while True:
            for k, v in jobs.items():
                if v.isActive:
                    print('Job: {} Progress: {:7.3f}%'.format(k, v.progressPercent))
            sleep(30)
    except (KeyboardInterrupt, SystemExit):
        running = False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost',LISTENING_PORT))
            s.close()
        except:
            pass
        print('Exiting')
        #sp.terminate()
        with open('server.dat','wb') as f:
            pickle.dump(jobs, f)

    listener.stop()
    
