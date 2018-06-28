from mplogger import *
import socket, threading, pickle
from connection import Connection

class Client(threading.Thread):
    def __init__(self, config, socket, game):
        super(Client, self).__init__()
        self.config = config
        self.socket = Connection(config, socket[0])
        self.address = socket[1]
        self.host = self.address[0]
        self.deamon = True
        self.game = game
    
    def run(self):
        logging.config.dictConfig(self.config)
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Connected to {} on port {}'.format(self.host, self.address[1]))
        while True:
            # Will block here until data is received
            m = self.socket.recv()  # Returns a Message object
            self.logger.error('Host {} sent {}'.format(self.host, m))
            if m.message.upper() == 'GET_DATA':
                self.socket.sendall(pickle.dumps(self.game))
            if m.message.upper() == 'CLOSE':
                break
            if m.message.upper() == 'CLIENT_INFO':
                self.logger.debug('Client version is {}'.format(m.params['VERSION']))
        self.logger.error('Closing connection with {}'.format(self.host))
        self.socket.close()
        
        