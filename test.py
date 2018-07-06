from mplogger import *
from job import Job
from game import *
from copy import deepcopy

g = Lotto()
g.load('lotto.csv')
j = Job(config = listener_config, game = g, pick_size = 6)
j.prep()
    
