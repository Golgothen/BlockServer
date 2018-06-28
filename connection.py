import socket, pickle
from message import Message
from mplogger import *

BUFFER_SIZE = 8192
SIZE_HEADER = 8
BYTE_ORDER = 'big'

__version__ = '0.0.1'

class Connection():
    def __init__(self, config, sock = None):
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)
        print(__name__)
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
        self.send(Message('CLIENT_INFO', VERSION = __version__))
    
    def send(self, data):
        if type(data).__name__ != 'Message':
            raise TypeError('Data must be in a Message object')
        self.logger.debug('Sending {}'.format(data))
        dump = pickle.dumps(data)
        size = len(dump).to_bytes(SIZE_HEADER, BYTE_ORDER)
        self.logger.debug('Message size is {}'.format(size))
        self.bytesSent += self.socket.send(size + dump)
        self.logger.debug('{} bytes sent'.format(self.bytesSent))
    
    def recv(self):
        messageSize = int.from_bytes(self.socket.recv(SIZE_HEADER), BYTE_ORDER)
        self.logger.debug('Incoming message size is {}'.format(messageSize))
        data = b''
        while len(data) < messageSize:
            data += self.socket.recv(BUFFER_SIZE)
        self.logger.debug('Message was {} bytes in length'.format(len(data)))
        self.bytesReceived += (len(data) + SIZE_HEADER)
        d = pickle.loads(data)
        self.logger.debug('Received {}'.format(d))
        return d
    
    def close(self):
        self.socket.close()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    