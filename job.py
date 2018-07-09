import multiprocessing, logging, logging.config
from itertools import combinations
from datetime import datetime, timedelta
from vector import vector
from game import *
from time import sleep

RECOMMENDED_CLIENT_BLOCK_SIZE = 5

class Job():
    def __init__(self, *args, **kwargs):
        self.game = None
        self.config = None
        #self.que = multiprocessing.Queue()
        self.allocated = {}
        self.returnUnallocated = []
        self.recycledBlocks = []
        self.blockSize = 0
        self.pickSize = 0
        self.totalBlocks = 0
        self.maxWait = timedelta(1, 0, 0)
        self.currentBest = Result()
        self.currentMost = Result()
        self.combinations = 0
        self.elapsed = 0
        self.completedCount = 0
        self.paused = False
        self.isAvailable = False
        #self.block_iter = None

        for k, v in kwargs.items():
            if k.lower() == 'config':
                self.config = v
                logging.config.dictConfig(v)
                self.logger = logging.getLogger(__name__)
            if k.lower() == 'game':
                self.game = v
            if k.lower() == 'block_size':
                self.blockSize = v
            if k.lower() == 'pick_size':
                self.pickSize = v
        
        if self.game is not None:
            if self.pickSize < self.game.minPick:
                self.pickSize = self.game.minPick
            if self.blockSize < (self.pickSize - RECOMMENDED_CLIENT_BLOCK_SIZE):
                self.blockSize = self.pickSize - RECOMMENDED_CLIENT_BLOCK_SIZE

        fn = self.game.poolSize - self.blockSize
        for i in range(1,self.game.poolSize - self.blockSize):
            fn *= i
        fr = self.pickSize
        for i in range(1,self.pickSize):
            fr *= i
        fd = self.game.poolSize - self.blockSize - self.pickSize
        for i in range(1, self.game.poolSize - self.pickSize):
            fd *= i
         
        self.totalBlocks = fn/(fr*fd)
        
        self.returnUnallocated = []
        self.allocated = {}
        self.currentBest = Result()
        self.currentMost = Result()
        self.combinations = 0
        self.elapsed = 0
        self.lastBlock = 0
        self.iter = combinations(range(1,self.game.poolSize + 1 - self.blockSize),self.blockSize)
        self.logger.info('Loaded job {}{} with {:12,.0f} blocks'.format(type(self.game).__name__, self.pickSize, self.totalBlocks))
        self.isAvailable = True
    
    def __getstate__(self):
        d = {}
        d['game'] = self.game
        d['allocated'] = self.allocated
        d['returnUnallocated'] = self.returnUnallocated
        d['block_size'] = self.blockSize
        d['combinations'] = self.combinations
        d['elapsed'] = self.elapsed
        d['pick_size'] = self.pickSize
        d['max_wait'] = self.maxWait
        d['total_blocks'] = self.totalBlocks
        d['current_best'] = self.currentBest
        d['current_most'] = self.currentMost
        d['last_block'] = self.lastBlock
        return d
    
    def __setstate__(self, d):
        self.paused = False
        self.game = d['game']
        self.allocated = d['allocated']
        if 'returnUnallocated' in d:
            self.returnUnallocated = d['returnUnallocated']
        else:
            self.returnUnallocated = []
        if 'combinations' in d:
            self.combinations = d['combinations']
        else:
            self.combinations = 0
        if 'elapsed' in d:
            self.elapsed = d['elapsed']
        else:
            self.elapsed = 0
        self.blockSize = d['block_size']
        self.pickSize = d['pick_size']
        self.maxWait = d['max_wait']
        self.totalBlocks = d['total_blocks']
        self.currentBest = d['current_best']
        self.currentMost = d['current_most']
        self.lastBlock = d['last_block']
        self.iter = combinations(range(1,self.game.poolSize + 1 - self.blockSize),self.blockSize)
        while True:
            try:
                if next(self.iter) == self.lastBlock:
                    break
            except StopIteration:
                self.isAvailable = False
                break
        
    def setLogger(self, config):
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)

    @property
    def isActive(self):
        if len(self.allocated) > 0 and self.isAvailable:
            return True
        return False
    
    def get(self):
        while True:
            try:
                block = next(self.iter)
                if block in self.returnUnallocated:
                    self.logger.info('Block {} already submitted'.format(block))
                    del self.returnUnallocated[self.returnUnallocated.index(block)]
                else:
                    break
            except StopIter:
                self.isAvailable = False
                return None
        self.logger.info('{}{}=========>>>{}'.format(type(self.game).__name__, self.pickSize, block))
        self.allocated[block] = datetime.now()
        return block, self.pickSize, self.currentBest, self.currentMost

    def submit(self, resultType, result):
        if resultType == 'BEST':
            if result > self.currentBest:
                self.logger.debug('New BEST result set to {}'.format(result))
                self.currentBest = result
                with open('{}-{}_{}.txt'.format(type(self.game).__name__, self.pickSize, resultType),'a') as f:
                    #if 'POWERBALL' in m.params:
                    #    f.write('Numbers = {} PB = {}, Divisions = {}, Weight = {}.\n'.format(m.params['NUMBERS'], m.params['POWERBALL'], m.params['DIVISIONS'], m.params['WEIGHT']))
                    #else:
                    f.write('Numbers = {}, Divisions = {}.\n'.format(result.numbers, result.divisions))
        if resultType == 'MOST':
            if result > self.currentMost:
                self.logger.debug('New MOST result set to {}'.format(result))
                self.currentMost = result
                with open('{}-{}_{}.txt'.format(type(self.game).__name__, self.pickSize, resultType),'a') as f:
                    #if 'POWERBALL' in m.params:
                    #    f.write('Numbers = {} PB = {}, Divisions = {}, Weight = {}.\n'.format(m.params['NUMBERS'], m.params['POWERBALL'], m.params['DIVISIONS'], m.params['WEIGHT']))
                    #else:
                    f.write('Numbers = {}, Divisions = {}.\n'.format(result.numbers, result.divisions))

    def complete(self, block, elapsed, combinations):
        self.logger.info('{}{}<<<========={}'.format(type(self.game).__name__, self.pickSize, block))
        if block in self.allocated:
            self.logger.debug('Block {} found in allocated list. Deleting.'.format(block))
            del self.allocated[block]
            self.completedCount += 1
        else:
            self.logger.info('Block {} not allocated'.format(block))
            self.returnUnallocated.append(block)
            self.completedCount += 1
        self.combinations += combinations
        self.elapsed += elapsed

    def recycle(self):
        for k, v in self.allocated.items():
            if (datetime.now() - v) > self.maxWait:
                self.logger.debug('Adding {} back to the work que'.format(k))
                self.recycledBlocks.append(k)
        for d in self.recycledBlocks:
            del self.allocated[d]
        self.logger.debug('{} blocks currently allocated'.format(len(self.allocated)))

    def flush(self):
        for k, v in self.allocated.items():
            if type(k).__name__ == 'tuple':
                self.logger.info('Adding {} back to the work que'.format(k))
                self.recycledBlocks.append(k)
        for d in self.recycledBlocks:
            del self.allocated[d]
        self.logger.debug('{} blocks currently allocated'.format(len(self.allocated)))
    
    @property
    def progressPercent(self):
        return (1-(self.completedCount / self.totalBlocks)) * 100

    @property
    def stats(self):
        return self.elapsed, self.combinations
    
    @property
    def blocksRemaining(self):
        return self.totalBlocks - self.completedCount

    @property
    def blocksAllocated(self):
        return len(self.allocated)
    