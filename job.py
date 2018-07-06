import multiprocessing, logging, logging.config
from itertools import combinations
from datetime import datetime, timedelta
from vector import vector
from game import *

RECOMMENDED_CLIENT_BLOCK_SIZE = 4

class Job():
    def __init__(self, *args, **kwargs):
        self.game = None
        self.config = None
        self.que = multiprocessing.Queue()
        self.allocated = {}
        self.returnUnallocated = []
        self.blockSize = 0
        self.pickSize = 0
        self.totalBlocks = 0
        self.maxWait = timedelta(1, 0, 0)
        self.currentBest = Result()
        self.currentMost = Result()
        self.combinations = 0
        self.elapsed = 0
        self.paused = False
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

    def prep(self):
            # Load the work que
            for i in combinations(range(1,self.game.poolSize + 1 - self.blockSize),self.blockSize):
                self.que.put(i)
                self.totalBlocks += 1
            self.logger.info('Loaded job {}{} with {:12,.0f} blocks'.format(type(self.game).__name__, self.pickSize, self.totalBlocks))
    
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
        d['block_que'] = []
        while not self.que.empty():
            d['block_que'].append(self.que.get())
        return d
    
    def __setstate__(self, d):
        self.que = multiprocessing.Queue()
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
        for b in d['block_que']:
            self.que.put(b)
    
    def __deepcopy__(self, memo):
        #print('Copy of Job')
        d = type(self)(config = self.config, game = self.game, pick_size = self.pickSize)
        d.allocated = self.allocated.copy()
        d.returnUnallocated = self.returnUnallocated.copy()
        d.blockSize = self.blockSize
        d.maxWait = self.maxWait
        d.totalBlocks = self.totalBlocks
        d.currentBest = self.currentBest
        d.currentMost = self.currentMost
        d.elapsed = self.elapsed
        d.combinations = self.combinations
        #self.paused = True
        self.que.put(None)
        while True:
            b = self.que.get()
            if b is None:
                break
            d.que.put(b)
            self.que.put(b)
        self.paused = False
        return d
        
    def setLogger(self, config):
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)

    @property
    def isActive(self):
        if self.que.qsize() > 0:
            return True
        if len(self.allocated) > 0:
            return True
        return False
    
    @property
    def isAvailable(self):
        if self.que.qsize() > 0:
            return True
        return False
    
    def get(self):
        if self.isAvailable:
            while self.paused:
                sleep(0.1)
            while True:
                block = self.que.get()
                if block in self.returnUnallocated:
                    #self.logger.info('Block {} already submitted'.format(block))
                    del self.returnUnallocated[self.returnUnallocated.index(block)]
                else:
                    break
            #self.logger.info('{}{}=========>>>{}'.format(type(self.game).__name__, self.pickSize, block))
            self.allocated[block] = datetime.now()
            return block, self.pickSize, self.currentBest, self.currentMost
        else:
            return None

    def submit(self, resultType, result):
        if resultType == 'BEST':
            if result > self.currentBest:
                self.logger.debug('New BEST result set to {}'.format(result))
                self.currentBest = result
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
        else:
            self.logger.debug('Block {} not allocated'.format(block))
            self.returnUnallocated.append(block)
        self.combinations += combinations
        self.elapsed += elapsed

    def recycle(self):
        deletedBlocks = []
        for k, v in self.allocated.items():
            if (datetime.now() - v) > self.maxWait:
                self.logger.debug('Adding {} back to the work que')
                self.que.put(k)
                deletedBlocks.append(k)
        for d in deletedBlocks:
            del self.allocated[d]
        self.logger.debug('{} blocks currently allocated'.format(len(self.allocated)))
    
    @property
    def progressPercent(self):
        return (1-((self.que.qsize() - len(self.returnUnallocated) + len(self.allocated)) / self.totalBlocks)) * 100

    @property
    def stats(self):
        return self.elapsed, self.combinations
    
    def purge(self):
        while not self.que.empty():
            self.que.get()
    
    @property
    def blocksRemaining(self):
        return self.que.qsize() + len(self.allocated)

    @property
    def blocksQueued(self):
        return self.que.qsize()
    
    @property
    def blocksAllocated(self):
        return len(self.allocated)
    