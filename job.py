import multiprocessing, logging, logging.config
from itertools import combinations
from datetime import datetime, timedelta
from vector import vector
from game import *
from time import sleep
from math import factorial

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
        #self.totalBlocks = 0
        self.maxWait = timedelta(1, 0, 0)
        self.currentBest = Result()
        self.currentMost = Result()
        self.combinations = 0
        self.elapsed = 0
        self.completedBlocks = 0
        self.paused = False
        self.iterAvailable = False
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

        #fn = self.game.poolSize - self.blockSize
        #for i in range(1,self.game.poolSize - self.blockSize):
        #    fn *= i
        #fr = self.blockSize
        #for i in range(1,self.blockSize):
        #    fr *= i
        #fd = self.game.poolSize - (2 * self.blockSize)
        #for i in range(1, self.game.poolSize - (2 * self.blockSize)):
        #    fd *= i
        # 
        #self.totalBlocks = fn/(fr*fd)
        
        self.returnUnallocated = []
        self.allocated = {}
        self.currentBest = Result()
        self.currentMost = Result()
        self.combinations = 0
        self.elapsed = 0
        self.lastBlock = None
        self.iter = combinations(range(1,self.game.poolSize + 1 - (RECOMMENDED_CLIENT_BLOCK_SIZE)),self.blockSize)
        #self.logger.info('Loaded job {}{} with {:12,.0f} blocks'.format(type(self.game).__name__, self.pickSize, self.totalBlocks))
        self.iterAvailable = True
    
    def __getstate__(self):
        d = {}
        d['game'] = self.game
        d['allocated'] = self.allocated
        d['returnUnallocated'] = self.returnUnallocated
        d['recycled_blocks'] = self.recycledBlocks
        d['block_size'] = self.blockSize
        d['combinations'] = self.combinations
        d['elapsed'] = self.elapsed
        d['pick_size'] = self.pickSize
        d['max_wait'] = self.maxWait
        #d['total_blocks'] = self.totalBlocks
        d['current_best'] = self.currentBest
        d['current_most'] = self.currentMost
        d['last_block'] = self.lastBlock
        d['iter_available'] = self.iterAvailable
        d['completed_blocks'] = self.completedBlocks

        return d
    
    def __setstate__(self, d):
        self.paused = False
        self.game = d['game']
        self.allocated = d['allocated']
        self.returnUnallocated = d['returnUnallocated']
        self.recycledBlocks = d['recycled_blocks']
        self.blockSize = d['block_size']
        self.combinations = d['combinations']
        self.elapsed = d['elapsed']
        self.pickSize = d['pick_size']
        self.maxWait = d['max_wait']
        #self.totalBlocks = d['total_blocks']
        self.currentBest = d['current_best']
        self.currentMost = d['current_most']
        self.lastBlock = d['last_block']
        self.iterAvailable = d['iter_available']
        self.completedBlocks = d['completed_blocks']
        
        if self.iterAvailable:
            self.iter = combinations(range(1,self.game.poolSize + 1 - self.blockSize),self.blockSize)
            if self.lastBlock is not None: 
                while True:
                    try:
                        if next(self.iter) == self.lastBlock:
                            break
                    except StopIteration:
                        if len(self.recycledBlocks) == 0:
                            self.isAvailable = False
                        break

    def setLogger(self, config):
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)

    @property
    def isActive(self):
        if len(self.allocated) > 0 or self.isAvailable or len(self.recycledBlocks) > 0:
            return True
        return False
    
    @property
    def isAvailable(self):
        if len(self.recycledBlocks) > 0:
            return True
        return self.iterAvailable
        
    def get(self):
        while True:
            if len(self.recycledBlocks) > 0:
                block = self.recycledBlocks.pop(0)
                break
            else:
                try:
                    if self.iterAvailable:
                        block = next(self.iter)
                        if block in self.returnUnallocated:
                            self.logger.info('Block {} already submitted'.format(block))
                            del self.returnUnallocated[self.returnUnallocated.index(block)]
                        else:
                            break
                except StopIteration:
                    self.iterAvailable = False
                    return None, None, None, None
        self.logger.info('{}{}=========>>>{}'.format(type(self.game).__name__, self.pickSize, block))
        self.allocated[block] = datetime.now()
        self.lastBlock = block
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
            self.completedBlocks += 1
        else:
            self.logger.info('Block {} not allocated'.format(block))
            self.returnUnallocated.append(block)
            self.completedBlocks += 1
        self.combinations += combinations
        self.elapsed += elapsed

    def recycle(self):
        for k, v in self.allocated.items():
            if (datetime.now() - v) > self.maxWait:
                self.logger.debug('Adding {} back to the work que'.format(k))
                self.recycledBlocks.append(k)
        for d in self.recycledBlocks:
            if d in self.allocated:
                del self.allocated[d]
        self.logger.debug('{} blocks currently allocated'.format(len(self.allocated)))

    def flush(self):
        for k, v in self.allocated.items():
            self.logger.debug('Adding {} back to the work que'.format(k))
            self.recycledBlocks.append(k)
        for d in self.recycledBlocks:
            if d in self.allocated:
                del self.allocated[d]
        self.logger.debug('{} blocks currently allocated'.format(len(self.allocated)))
    
    @property
    def progressPercent(self):
        return (self.completedBlocks / self.size) * 100

    @property
    def stats(self):
        return self.elapsed, self.combinations
    
    @property
    def blocksRemaining(self):
        return self.size - self.completedBlocks

    @property
    def blocksAllocated(self):
        return len(self.allocated)
    
    @property
    def size(self):
        return factorial(self.game.poolSize - RECOMMENDED_CLIENT_BLOCK_SIZE)/(factorial(self.blockSize)*factorial(self.game.poolSize - RECOMMENDED_CLIENT_BLOCK_SIZE - self.blockSize))
