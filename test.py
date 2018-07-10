from job import Job
from game import *

g = Lotto()
g.load('lotto.csv')
j = Job(game = g, pick_size = 6)
j.prep()
    
