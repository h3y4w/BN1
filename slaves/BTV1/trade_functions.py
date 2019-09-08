"""
Will hold algo for trading functions"
"""
def extract_tag_info(tag):
    #ex 'bse+bnbbtc/ticker'
    raw = tag.split('+')[1]
    pair, pair_route = raw.split('/')
    return pair, pair_route


class StreamTagData (object):
    pair = None
    pair_route = None

    current = None
    past = None 
    PAST_MAX_LENGTH = 60

    unique_id_key = None

    cb_func = None
    def __init__(self, pair, pair_route, cb_func=None, obj_key=None, max_length=60):
        self.pair = pair
        self.pair_route = pair_route
        self.past = []
        self.cb_func = cb_func
        self.PAST_MAX_LENGTH = max_length
        self.obj_key = obj_key
        if self.obj_key:
            self.table = {}

    def cb(self):
        if self.cb_func:
            return self.cb_func(self.all())

    def all(self):
        if self.current:
            return [self.current] + self.past
        return []
    @staticmethod
    def __init_from_stream_tag__(stream_tag):
        pair, pair_route = extract_tag_info(stream_tag)
        return StreamTagData(pair, pair_route)

    def insert(self, row):
        if not self.current:
            self.current = row
            return

        if row['stream'] != self.current['stream']:
            raise ValueError('Stream types are not equal')

        key = 'lastUpdateId'
        if not key in row.keys():
            key = 'E'

        if row[key] > self.current[key]: #sets the most current ticker
            old_row = self.current
            self.current = row
            row = old_row #now its not the most currect trd so it will check to see where it will add in list

        added = False
        idx = 0
        for idx in xrange(0, len(self.past)):
            if row[key] > self.past[idx][key]:
                break
        self.past.insert(idx, row)

        if self.PAST_MAX_LENGTH <= len(self.past):
            self.past.pop()
    
    def past(self):
        if not self.obj_key:
            return self.past

        #else if updating
        l = len(self.past)
        if l:
            j = l-1
            for i in xrange(0, l):
                obj = self.past[i]
                id_ = obj[self.obj_key]['t']
                if not id_ in self.table.keys():
                    #add if not in table
                    self.table[id_] = obj

                elif self.table[id_]['E'] < obj['E']:
                    #update if in table n Event time is greater
                    self.table[id_] = obj
                else:
                    self.past.pop(i)
                    i-=1
                    pass

        



