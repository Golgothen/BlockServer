import multiprocessing, logging, logging.config
from itertools import combinations
from datetime import datetime, timedelta
from vector import vector

RECOMMENDED_CLIENT_BLOCK_SIZE = 4

class Result():
    def __init__(self):
        self.numbers = None
        self.divisions = None
    
    def __gt__(self, r):
        if self.divisions is None:
            return False
        if r.divisions is None:
            return True
        return sum(self.divisions) > sum(r.divisions)
    
    def __str__(self):
        return 'Numbers: {}; Divisions: {}'.format(self.numbers, self.divisions)

    def __repr__(self):
        return str(self)
        
class Job():
    def __init__(self, *args, **kwargs):
        self.__game = None
        self.__que = multiprocessing.Queue()
        self.__allocated = {}
        self.__blockSize = 0
        self.__pickSize = 0
        self.__totalBlocks = 0
        self.__maxWait = timedelta(7, 0, 0)
        self.__currentBest = Result()
        self.__currentMost = Result()
        self.__block_iter = None

        for k, v in kwargs.items():
            if k.lower() == 'config':
                logging.config.dictConfig(v)
                self.logger = logging.getLogger(__name__)
            if k.lower() == 'game':
                self.__game = v
            if k.lower() == 'block_size':
                self.__blockSize = v
            if k.lower() == 'pick_size':
                self.__pickSize = v
        
        if self.__game is not None:
            if self.__pickSize < self.__game.minPick:
                self.__pickSize = self.__game.minPick
            if self.__blockSize < (self.__pickSize - RECOMMENDED_CLIENT_BLOCK_SIZE):
                self.__blockSize = self.__pickSize - RECOMMENDED_CLIENT_BLOCK_SIZE

            # Load the work que
            for i in combinations(range(1,self.__game.poolSize + 1 - self.__blockSize),self.__blockSize):
                self.__que.put(i)
                self.__totalBlocks += 1
    
    def __getstate__(self):
        d = {}
        d['game'] = self.__game
        d['allocated'] = self.__allocated
        d['block_size'] = self.__blockSize
        d['pick_size'] = self.__pickSize
        d['max_wait'] = self.__maxWait
        d['total_blocks'] = self.__totalBlocks
        d['current_best'] = self.__currentBest
        d['current_most'] = self.__currentMost
        d['block_que'] = []
        while not self.__que.empty():
            d['block_que'].append(self.__que.get())
        return d
    
    def __setstate__(self, d):
        self.__game = d['game']
        self.__allocated = d['allocated']
        self.__blockSize = d['block_size']
        self.__pickSize = d['pick_size']
        self.__maxWait = d['max_wait']
        self.__totalBlocks = d['total_blocks']
        self.__currentBest = d['current_best']
        self.__currentMost = d['current_most']
        self.__que = multiprocessing.Queue()
        for b in d['block_que']:
            self.__que.put(b)
    
    def setLogger(self, config):
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)

    @property
    def isActive(self):
        if self.__que.qsize() > 0:
            return True
        if len(self.__allocated) > 0:
            return True
        return False
    
    @property
    def isAvailable(self):
        if self.__que.qsize() > 0:
            return True
        return False
    
    def get(self):
        if self.isAvailable:
            block = self.__que.get()
            self.logger.debug('Returning block {}'.format(block))
            self.__allocated[block] = datetime.now()
            return block, self.__currentBest, self.__currentMost
        else:
            return None

    def submit(self, block, bestResult, mostResult):
        self.logger.debug('Received block {}'.format(block))
        if block in self.__allocated:
            self.logger.debug('Block {} found in allocated list. Deleting.'.format(block))
            del self.__allocated[block]
        if bestResult > self.__currentBest:
            self.logger.debug('New BEST result set to {}'.format(bestResult))
            self.__currentBest = bestResult
        if mostResult > self.__currentMost:
            self.logger.debug('New MOST result set to {}'.format(mostResult))
            self.__currentMost = mostResult
        
    def recycle(self):
        deletedBlocks = []
        for k, v in self.__allocated:
            if (datetime.now() - v) > self.__maxWait:
                self.logger.debug('Adding {} back to the work que')
                self.__que.put(v)
                deletedBlocks.append(k)
        for d in deletedBlocks:
            del self.__allocated[d]
        self.logger.debug('{} blocks currently allocated'.format(len(self.__allocated)))
    
    @property
    def progressPercent(self):
        return self.__que.qsize() / self.__totalBlocks * 100
    
    def purge(self):
        while not self.__que.empty():
            self.__que.get()