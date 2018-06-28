class Message():
    def __init__(self, message, **kwargs):
        self.message = message
        self.params = dict()
        for k in kwargs:
            self.params[k] = kwargs[k]

    def __str__(self):
        s = '{} :'.format(self.message)
        for k in self.params.keys():
            s += ' {} = {};'.format(k, self.params[k])
        return s
    
    def __repr__(self):
        return str(self)
    