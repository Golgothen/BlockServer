from datetime import datetime
from vector import vector
import csv

class Result():
    def __init__(self):
        self.numbers = None
        self.divisions = None
    
    def __gt__(self, r):
        if self.divisions is None:
            return False
        if r.divisions is None:
            return True
        return sum(self.divisions) > sum(r.divisions)
    
    def __str__(self):
        return 'Numbers: {}; Divisions: {}'.format(self.numbers, self.divisions)

    def __repr__(self):
        return str(self)
    
    def __mul__(self, x):
        r = Result()
        r.numbers = self.numbers
        r.divisions = self.divisions * x
        return r

class Game():
    #
    # Base class for other game types
    #
    def __init__(self, poolSize = None):
        self.games = []
        if poolSize is None:
            self.poolSize = 45
        else:
            self.poolSize = poolSize
        
    def load(self, filename):
        #
        # Implement this in subclasses
        #
        pass
    
    def play(self, numbers):
        #
        # Implement this in subclasses
        #
        pass
    
    @property
    def len(self):
        return len(self.games)


class Lotto(Game):
    def __init__(self):
        super().__init__(45)
        self.divisionWeights = vector([95000, 750, 100, 3, 2, 1])
        self.minPick = 6
        self.maxPick = 20
        self.divisions = 6
    
    def load(self, filename, day = None):
        if day is None:
            day = ['SAT','MON','WED']
        with open(filename) as f:
            reader = csv.reader(f)
            next(reader)        # Discard the heading line
            for row in reader:
                if row[0].upper() in day:  # + ['WED','MON']
                    g = {}
                    g['Drawdate'] = datetime.strptime(row[1],"%d/%m/%Y")
                    g['Numbers'] = set([int(i) for i in row[2:8]])
                    g['Sups'] = set([int(i) for i in row[8:10]])
                    self.games.append(g)
    
    def play(self, numbers):
        result = Result()
        result.numbers = numbers
        result.divisions = vector(self.divisions)
        for g in self.games:
            drawcount = 0
            supcount = 0
            drawcount=len(g['Numbers'].intersection(numbers))
            supcount=len(g['Sups'].intersection(numbers))
            if drawcount == 6:
                result.division[0]+=1
                continue
            if drawcount == 5:
                if supcount > 0:
                    result.divisions[1]+=1
                    continue
                else:
                    result.divisions[2]+=1
                    continue
            if drawcount == 4:
                result.divisions[3]+=1
                continue
            if drawcount == 3 and supcount > 0:
                result.divisions[4]+=1
                continue
            if drawcount > 0 and supcount == 2:
                result.divisions[5]+=1
        return result

class OzLotto(Game):
    def __init__(self):
        super().__init__(45)
        self.divisionWeights = vector([680000, 1550, 200, 20, 3, 2, 1])
        self.minPick = 7
        self.maxPick = 18
        self.divisions = 7
    
    def load(self, filename, day = None):
        with open(filename) as f:
            reader = csv.reader(f)
            next(reader)        # Discard the heading line
            for row in reader:
                g = {}
                g['Drawdate'] = datetime.strptime(row[0],"%d/%m/%Y")
                g['Numbers'] = set([int(i) for i in row[1:8]])
                g['Sups'] = set([int(i) for i in row[8:10]])
                self.games.append(g)
    
    def play(self, numbers):
        result = {}
        result['Divisions'] = vector(self.divisions)
        for g in self.games:
            drawcount = 0
            supcount = 0
            drawcount=len(g['Numbers'].intersection(numbers))
            supcount=len(g['Sups'].intersection(numbers))
            if drawcount == 7:
                result['Divisions'][0]+=1
                continue
            if drawcount == 6:
                if supcount > 0:
                    result['Divisions'][1]+=1
                    continue
                else:
                    result['Divisions'][2]+=1
                    continue
            if drawcount == 5:
                if supcount > 0:
                    result['Divisions'][3]+=1
                    continue
                else:
                    result['Divisions'][4]+=1
                    continue
            if drawcount == 4:
                result['Divisions'][5]+=1
                continue
            if drawcount == 3 and supcount > 0:
                result['Divisions'][6]+=1
                continue
        result['Weight'] = sum(result['Divisions']*self.divisionWeights) 
        return result 

class PowerBall(Game):
    def __init__(self):
        super().__init__(35)
        self.divisionWeights = vector([300000, 500, 20, 8, 2, 1])
        self.minPick = 7
        self.divisions = 6
        self.powerball = 18
    
    def load(self, filename, day = None):
        with open(filename) as f:
            reader = csv.reader(f)
            next(reader)        # Discard the heading line
            
            for row in reader:
                g = {}
                g['Drawdate'] = datetime.strptime(row[0],"%d/%m/%Y")
                g['Numbers'] = set([int(i) for i in row[1:8]])
                g['Powerball'] = int(row[8])
                self.games.append(g)
    
    def play(self, numbers):
        result = {}
        for i in range(self.powerball):
            result[i+1] = {'Divisions': vector(self.divisions), 'Weight' : 0}
        for g in self.games:
            drawcount=len(g['Numbers'].intersection(numbers))
            if drawcount > 2:
                result[g['Powerball']]['Divisions'][7-drawcount]+=1
        for k in result.keys():
            result[k]['Weight'] = sum(result[k]['Divisions']*self.divisionWeights)
        return result

class MegaMillions(Game):
    def __init__(self):
        super().__init__(70)
        self.divisionWeights = vector([300000, 500, 20, 8, 2, 1])
        self.minPick = 5
        self.divisions = 5
        self.powerball = 25
    
    def load(self, filename, day = None):
        if day is None:
            day = ['TUE','FRI']
        with open(filename) as f:
            reader = csv.reader(f)
            next(reader)        # Discard the heading line
            for row in reader:
                if row[1].upper() in day:
                    g = {}
                    g['Drawdate'] = datetime.strptime(row[0],"%a, %b %d, %Y")
                    g['Numbers'] = set([int(i) for i in row[2:7]])
                    g['Powerball'] = int(row[7])
                    print(g)
                    self.games.append(g)
    
    def play(self, numbers):
        result = {}
        for i in range(self.powerball):
            result[i+1] = {'Divisions': vector(self.divisions), 'Weight' : 0}
        for g in self.games:
            drawcount=len(g['Numbers'].intersection(numbers))
            if drawcount > 0:
                result[g['Powerball']]['Divisions'][self.divisions-drawcount]+=1
        for k in result.keys():
            result[k]['Weight'] = sum(result[k]['Divisions']*self.divisionWeights)
        return result

class USPowerBall(Game):
    def __init__(self):
        super().__init__(69)
        self.divisionWeights = vector([300000, 500, 20, 8, 2, 1])
        self.minPick = 5
        self.divisions = 5
        self.powerball = 26
    
    def load(self, filename, day = None):
        if day is None:
            day = ['WED','SAT']
        with open(filename) as f:
            reader = csv.reader(f)
            next(reader)        # Discard the heading line
            for row in reader:
                if row[1].upper() in day:
                    g = {}
                    g['Drawdate'] = datetime.strptime(row[0],"%a, %b %d, %Y")
                    g['Numbers'] = set([int(i) for i in row[2:7]])
                    g['Powerball'] = int(row[7])
                    self.games.append(g)
    
    def play(self, numbers):
        result = {}
        for i in range(self.powerball):
            result[i+1] = {'Divisions': vector(self.divisions), 'Weight' : 0}
        for g in self.games:
            drawcount=len(g['Numbers'].intersection(numbers))
            if drawcount > 0:
                #print(result)
                #print(g)
                result[g['Powerball']]['Divisions'][self.divisions-drawcount]+=1
        for k in result.keys():
            result[k]['Weight'] = sum(result[k]['Divisions']*self.divisionWeights)
        return result
