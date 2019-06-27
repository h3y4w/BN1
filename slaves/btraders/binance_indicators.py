from indicators import Indicator

class UpMomentum (Indicator):
    weight = 15
    def __init__(self):
        pass

    def check_func(self, tds):
        targeted = []
        l = len(tds)
        if l < 60:
            return False

        for i in xrange(0, 6):
            for j in xrange(0, 10):
                pass

            if i < 3:
                targeted.append(float(tds[i]['c']))
                continue
            p = float(tds[i]['c'])
            #if p


            

        tds[-1]
        f
class FutureDemand(Indicator):
    weight = 45
    def __init__(self):
        pass

    def check_func(self, items):
        for i in xrange(0, len(items)):
            if i == 10:
                break

            #item[0]['bids']

        #print "items: {}".format(items)
        return False
        

def possibleUptrend (items, ind_3crows, ind_invhammer):
    if ind_3crows.check_func(items) and ind_invhammer.check_func(items):
        return ind_3crows.weight + ind_invhammer.weight
    return 0

class Kline_3_crows (Indicator):
    weight = 100
    def __init__(self):
        pass

    def check_func(self, candles):
        if len(candles) < 3:
            return False

        active = 0 
        i=0
        for i in xrange(1, len(candles)):
            c0 = candles[i-1]['k']
            c1 = candles[i]['k']

            if active > 2:
                break
            if float(c0['o']) > float(c0['c']):
                if float(c1['o']) > float(c1['c']):
                    if float(c0['c']) >= float(c1['c']):
                        active+=1
                        continue

            active = 0

        if active > 2:
            print '3 crows t: {}--------\n'.format(candles[i]['k']['t'])

        return active > 2
        


class Kline_3_soldiers (Indicator):
    weight = -10

    def __init__(self):
        pass

    def check_func(self, candles):
        if len(candles) < 3:
            return False

        for i in xrange(0, 3):
            c = candles[i]['k']
            if not c['c'] > c['o']:
                return False
        
        for i in xrange(0, 3):
            c = candles[i]['k']
            print c

        print '--------\n'
        return False 

class Kline_hammer_inverted (Indicator):
    weight = -31
    
    def __init__(self):
        pass

    def check_func(self, candles):
        c = candles[0]['k']
        lower_v = float(c['c'])
        higher_v = float(c['o'])
        if float(c['o']) < float(c['c']):
            lower_v = float(c['o'])
            higher_v = float(c['c'])

        low_pct = float(c['l'])/lower_v 
        high_pct = float(c['h'])/higher_v

        if low_pct > .80 and high_pct < .30:
            print "\n-------\nKline Inverted Hammer!"
            return True


class Kline_hammer (Indicator):
    weight = 10
  
    def __init__(self):
        pass
    def check_func(self, candles):
        c = candles[0]['k']
        lower_v = float(c['c'])
        higher_v = float(c['o'])
        if float(c['o']) < float(c['c']):
            lower_v = float(c['o'])
            higher_v = float(c['c'])

        low_pct = float(c['l'])/lower_v 
        high_pct = float(c['h'])/higher_v

        if high_pct > .80 and low_pct < .30:
            print "\n-------\nKline Hammer!"
            return True
        return False

        c_len = float(c['h']) - float(c['l'])
        head_len = c_len - lower_v
        handle_len = c_len - head_len

        if (c_len): 
            print 'hammer: {}'.format((handle_len/c_len) >= .80)
            return (handle_len/c_len) >= .80
    '''
    def check_func(self, candles):
        c = candles[0]['k']
        v = float(c['c'])
        if float(c['o']) < float(c['c']):
            v = float(c['o'])
        
        c_len = float(c['h']) - float(c['l'])
        head_len = c_len - v
        handle_len = c_len - head_len
        if (c_len): 
            print 'hammer: {}'.format((handle_len/c_len) >= .80)
            return (handle_len/c_len) >= .80
    '''



class Resistance (Indicator):
    enum = "Resistance"

    pass

class Support (Indicator):
    enum = "Support"
    pass

