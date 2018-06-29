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
        #self.send(Message('CLIENT_INFO', VERSION = __version__))
        #m = self.recv()
        #if m.message != 'OK':
        #    raise RuntimeError('Server reports incompatible client.')
    
    def send(self, data):
        self.logger.debug('Sending {}'.format(data))
        dump = pickle.dumps(data)
        size = len(dump).to_bytes(SIZE_HEADER, byteorder = BYTE_ORDER)
        self.logger.debug('Message size is {}'.format(len(dump)))
        sent = self.socket.send(size + dump)
        self.logger.debug('{} bytes sent'.format(sent))
        self.bytesSent += sent
        self.logger.debug('{} total bytes sent'.format(self.bytesSent))
    
    def recv(self):
        data = self.socket.recv(SIZE_HEADER)
        #if not data:
        #    return(Message('CLOSE'))
        messageSize = int.from_bytes(data, byteorder = BYTE_ORDER)
        self.logger.debug('Message is {} bytes long'.format(messageSize))
        if messageSize <= BUFFER_SIZE:
            self.logger.debug('Pulling {} bytes from buffer'.format(messageSize))
            data = self.socket.recv(messageSize)
        else:
            data = b''
            while messageSize > BUFFER_SIZE:
                d = self.socket.recv(BUFFER_SIZE)
                self.logger.debug('Pulling {} bytes from buffer'.format(len(d)))
                messageSize -= len(d)
                data += d
                self.logger.debug('Pulled {} bytes so far'.format(len(data)))
                self.logger.debug('{} bytes to go'.format(messageSize))
            if messageSize > 0:
                d = self.socket.recv(messageSize)
                self.logger.debug('Pulling {} bytes from buffer'.format(len(d)))
                data += d
        self.logger.debug('Message was {} bytes in length'.format(len(data)))
        self.bytesReceived += (len(data) + SIZE_HEADER)
        self.logger.debug('{} total bytes received'.format(self.bytesReceived))
        d = pickle.loads(data)
        self.logger.debug('Received: {}'.format(d))
        return d
    
    def close(self):
        self.socket.close()
        #recreate the socket ready for the next connection
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    