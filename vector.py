import operator

class vector(list):
    def __init__(self, initSize = None):
        super(list, self).__init__()
        if initSize is not None:
            if type(initSize).__name__ == 'int':
                for a in range(initSize):
                    self.append(0)
            if type(initSize).__name__ in ['list', 'tuple', 'vector']:
                for a in initSize:
                    self.append(a)
    
    @property
    def mean(self):
        if len(self)>0:
            return sum(self)/len(self)
        else:
            return 0
    
    def sort(self, desc = True):
        s = vector()
        for i in range(self.size):
            s.append((i,self[i]))
        s = sorted(s, key = operator.itemgetter(1), reverse = desc)
        return s
    
    @property
    def size(self):
        return len(self)
    
    def __add__(self, v):
        #
        # Addition of vectors
        #
        r = vector()
        if type(v).__name__ == 'vector':        # If adding two vectors
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] + v[i])        # Add the two elements together
        else:                                   # If adding a scalar
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] + v)           # Add the scalar to each element
        return r                                # Return the new vector
    
    def __mul__(self, v):
        #
        # Multiplication of vectors
        #
        r = vector()
        if type(v).__name__ == 'vector':        # If multiplying two vectors
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] * v[i])        # Multiply the two elements together
        else:                                   # If multiplying a scalar
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] * v)           # Multiply each element by the scalar
        return r                                # Return the new vector

    def __sub__(self, v):
        #
        # Subtraction of vectors
        #
        r = vector()
        if type(v).__name__ == 'vector':        # If subtracting two vectors
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] - v[i])        # Subtract the two elements
        else:                                   # If subtracting a scalar
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] - v)           # Subtract the scalar from each element
        return r                                # Return the new vector

    def __truediv__(self, v):
        #
        # Addition of vectors
        #
        r = vector()
        if type(v).__name__ == 'vector':        # If dividing two vectors
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] / v[i])        # Divide the two elements
        else:                                   # If dividing by a scalar
            for i in range(self.size):          # Iterate through the vector elements
                r.append(self[i] / v)           # Divide each element by the scalar
        return r                                # Return the new vector
    
    def __pow__(self, v):
        r = vector()
        for i in range(self.size):
            r.append(self[i]**v)
        return r
