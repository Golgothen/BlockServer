import multiprocessing, logging, logging.config
from itertools import combinations
from datetime import datetime, timedelta
from vector import vector
from game import *

RECOMMENDED_CLIENT_BLOCK_SIZE = 4

class Job():
    def __init__(self, *args, **kwargs):
        self.__game = None
        self.__que = multiprocessing.Queue()
        self.__allocated = {}
        self.__blockSize = 0
        self.__pickSize = 0
        self.__totalBlocks = 0
        self.__maxWait = timedelta(1, 0, 0)
        self.__currentBest = Result()
        self.__currentMost = Result()
        self.combinations = 0
        self.elapsed = 0
        #self.__block_iter = None

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

    def prep(self):
            # Load the work que
            for i in combinations(range(1,self.__game.poolSize + 1 - self.__blockSize),self.__blockSize):
                self.__que.put(i)
                self.__totalBlocks += 1
            self.logger.info('Loaded job {}{} with {:12,.0f} blocks'.format(type(self.__game).__name__, self.__pickSize, self.__totalBlocks))
    
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
            self.logger.info('{}{}=========>>>{}'.format(type(self.__game).__name__, self.__pickSize, block))
            self.__allocated[block] = datetime.now()
            return block, self.__pickSize, self.__currentBest, self.__currentMost
        else:
            return None

    def submit(self, resultType, result):
        if resultType == 'BEST':
            if result > self.__currentBest:
                self.logger.debug('New BEST result set to {}'.format(result))
                self.__currentBest = result
        if resultType == 'MOST':
            if result > self.__currentMost:
                self.logger.debug('New MOST result set to {}'.format(result))
                self.__currentMost = result
        with open('{}-{}_{}.txt'.format(type(self.__game).__name__, self.__pickSize, resultType),'a') as f:
            #if 'POWERBALL' in m.params:
            #    f.write('Numbers = {} PB = {}, Divisions = {}, Weight = {}.\n'.format(m.params['NUMBERS'], m.params['POWERBALL'], m.params['DIVISIONS'], m.params['WEIGHT']))
            #else:
            f.write('Numbers = {}, Divisions = {}.\n'.format(result.numbers, result.divisions))

    def complete(self, block, elapsed, combinations):
        self.logger.info('{}{}<<<========={}'.format(type(self.__game).__name__, self.__pickSize, block))
        if block in self.__allocated:
            self.logger.debug('Block {} found in allocated list. Deleting.'.format(block))
            del self.__allocated[block]
        #self.combinations += combinations
        #self.elapsed += elapsed

    def recycle(self):
        deletedBlocks = []
        for k, v in self.__allocated.items():
            if (datetime.now() - v) > self.__maxWait:
                self.logger.debug('Adding {} back to the work que')
                self.__que.put(v)
                deletedBlocks.append(k)
        for d in deletedBlocks:
            del self.__allocated[d]
        self.logger.debug('{} blocks currently allocated'.format(len(self.__allocated)))
    
    @property
    def progressPercent(self):
        return (1-((self.__que.qsize() + len(self.__allocated)) / self.__totalBlocks)) * 100

    @property
    def stats(self):
        return self.elapsed, self.combinations
    
    def purge(self):
        while not self.__que.empty():
            self.__que.get()
    
    @property
    def game(self):
        return self.__game
    
    @property
    def pickSize(self):
        return self.__pickSize
    
    @property
    def blocksRemaining(self):
        return self.__que.qsize() + len(self.__allocated)

    @property
    def blocksQueued(self):
        return self.__que.qsize()
    
    @property
    def blocksAllocated(self):
        return len(self.__allocated)
    