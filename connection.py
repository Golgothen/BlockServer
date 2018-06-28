import socket, pickle
from message import Message
BUFFER_SIZE = 8192

__version__ = '0.0.1'

class Connection():
    def __init__(self, sock = None):
        if sock is None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.socket = sock
        self.bytesSent = 0
        self.bytesReceived = 0
        
    def connect(self, host, port):
        self.socket.connect((host, port))
        self.send(Message('CLIENT_INFO', version = __version__))
    
    def send(self, data):
        if type(data).__name__ != 'Message':
            raise TypeError('Data must be in a Message object')
        self.bytesSent += self.socket.send(pickle.dumps(data))
    
    def recv(self):
        data = b''
        while True:
            chunk = self.socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            data += chunk
        return pickle.loads(data)
    
    def close(self):
        self.socket.close()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    