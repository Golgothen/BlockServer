from mplogger import *
import threading
from connection import Connection
from message import Message

class Client(threading.Thread):
    def __init__(self, config, socket, jobs):
        super(Client, self).__init__()
        self.config = config
        self.socket = Connection(config = config, sock = socket[0])
        self.address = socket[1]
        self.host = self.address[0]
        self.deamon = True
        self.jobs = jobs
    
    def run(self):
        logging.config.dictConfig(self.config)
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Connected to {} on port {}'.format(self.host, self.address[1]))
        while True:
            self.logger.debug('Recv wait...')
            # Will block here until data is received
            try:
                m = self.socket.recv()  # Returns a Message object
            except ConnectionResetError:
                self.logger.error('Connection to {} was reset. Closing.'.format(self.host))
                self.socket.close()
                return
            self.logger.debug('Host {} sent {}'.format(self.host, m))
            if m.message.upper() == 'GET_DATA':
                self.socket.send(Message('GAME_DATA', GAME = self.jobs[m.params['GAMEID']].game))
            if m.message.upper() == 'GET_BLOCK':
                for k, j in self.jobs.items():
                    if j.isAvailable:
                        bl, pick, best, most = j.get()
                        self.socket.send(Message('BLOCK_DATA', BLOCK = bl, PICK = pick, BEST = best, MOST = most, GAME = type(j.game).__name__))
                        j.recycle()
                        if j.progressPercent > 95:
                            if j.pickSize + 1 <= j.game.maxPick:
                                if not self.jobs['{}{}'.format(type(j.game).__name__, j.pickSize+1)].isAvailable:
                                    self.jobs['{}{}'.format(type(j.game).__name__, j.pickSize+1)].prep()
                        break
            if m.message.upper() == 'CLOSE':
                break
            if m.message.upper() == 'CLIENT_INFO':
                self.logger.info('Client version is {}'.format(m.params['VERSION']))
                self.socket.send(Message('OK'))
            if m.message.upper() == 'RESULT':
                self.jobs['{}{}'.format(m.params['GAMEID'], m.params['PICK'])].submit(m.params['RESULT_TYPE'], m.params['RESULT'])
            if m.message.upper() == 'COMPLETE':
                self.jobs['{}{}'.format(m.params['GAMEID'], m.params['PICK'])].complete(m.params['BLOCK'], m.params['ELAPSED'], m.params['COMBINATIONS'], )
                
                
        self.logger.debug('Closing connection with {}'.format(self.host))
        self.socket.close()
        
        