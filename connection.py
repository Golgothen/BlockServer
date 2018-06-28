import socket, pickle
from message import Message
from mplogger import *

BUFFER_SIZE = 8192

__version__ = '0.0.1'

class Connection():
    def __init__(self, config, sock = None):
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)
        if sock is None:
            self.logger.debug('Creating a new socket')
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.logger.debug('Using passed in socket')
            self.socket = sock
        self.bytesSent = 0
        self.bytesReceived = 0
        
    def connect(self, host, port):
        self.socket.connect((host, port))
        self.logger.debug('Connected to {}:{}'.format(host, port))
        self.send(Message('CLIENT_INFO', version = __version__))
    
    def send(self, data):
        if type(data).__name__ != 'Message':
            raise TypeError('Data must be in a Message object')
        self.logger.debug('Sending {}'.format(data))
        self.bytesSent += self.socket.send(pickle.dumps(data))
    
    def recv(self):
        data = b''
        while True:
            chunk = self.socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            self.bytesReceived += len(chunk)
            data += chunk
            d = pickle.loads(data)
        self.logger.debug('Received {}'.format(d))
        return d
    
    def close(self):
        self.socket.close()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    