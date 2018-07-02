from mplogger import *
import threading
from connection import Connection
from message import Message

class Client(threading.Thread):
    def __init__(self, config, socket, job):
        super(Client, self).__init__()
        self.config = config
        self.socket = Connection(config, socket[0])
        self.address = socket[1]
        self.host = self.address[0]
        self.deamon = True
        self.job = job
    
    def run(self):
        logging.config.dictConfig(self.config)
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Connected to {} on port {}'.format(self.host, self.address[1]))
        while True:
            self.logger.debug('Recv wait...')
            # Will block here until data is received
            m = self.socket.recv()  # Returns a Message object
            self.logger.debug('Host {} sent {}'.format(self.host, m))
            if m.message.upper() == 'GET_DATA':
                self.socket.send(Message('GAME_DATA', GAME = self.job.game))
            if m.message.upper() == 'GET_BLOCK':
                bl, pick, best, most = self.job.get()
                self.socket.send(Message('BLOCK_DATA', BLOCK = bl, PICK = pick, BEST = best, MOST = most, GAME = type(self.job.game).__name__))
            if m.message.upper() == 'CLOSE':
                break
            if m.message.upper() == 'CLIENT_INFO':
                self.logger.debug('Client version is {}'.format(m.params['VERSION']))
                self.socket.send(Message('OK'))
            #if m.message.upper() == 'SUBMISSION':
                
        self.logger.debug('Closing connection with {}'.format(self.host))
        self.socket.close()
        
        