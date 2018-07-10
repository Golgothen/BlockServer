from message import Message
from mplogger import *
from connection import Connection

c = Connection(host = 'localhost', port = 2345, config = listener_config)

def completeBlocks(n):
    for i in range(n):
        c.send(Message('GET_BLOCK'))
        b = c.recv()
        c.send(Message('COMPLETE', BLOCK = b.params['BLOCK'], GAMEID = b.params['GAME'], PICK = b.params['PICK'], ELAPSED = 0, COMBINATIONS = 0))

def getBlocks(n):
    for i in range(n):
        c.send(Message('GET_BLOCK'))
        b = c.recv()
