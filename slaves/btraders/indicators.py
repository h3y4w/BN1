import copy
class IndicatorRelationship(object):
    generator_id = None
    indicator_ids = None
    id_ = None
    check_func = None
    multiplier = 1.0 
    one_to_one = False

    def set_id(self, id_):
        self.id_ = id_

    def set_generator_id(self, generator_id):
        self.generator_id = generator_id

    def set_multiplier(self, multiplier):
        self.multiplier = multiplier
    @staticmethod
    def __generator_init__(check_func, generator_id, id_, *indicators):
        ir = IndicatorRelationship(check_func, *indicators)
        ir.set_generator_id(generator_id)
        ir.set_id(id_)
        return ir

    def __init__(self, check_func, *indicators):
        self.indicator_ids = []
        self.check_func = check_func
        if not self.check_func:
            self.one_to_one = True

        self.multiplier = 1  

        indicators = list(indicators)
        print 'indicators: {}'.format(indicators)

        for indicator in indicators:
            if isinstance(indicator, Indicator):

                if not self.generator_id or self.generator_id == indicator.generator_id:
                    print 'adding: {}'.format(indicator.id_)
                    self.indicator_ids.append(indicator.id_)
                else:
                    raise ValueError("Different Generator id")
            else:
                raise TypeError("Not Indicator type: {}".format(type(indicator)))

    def get_all_ids(self):
        return self.indicator_ids
    
    def check(self, g, values):
        weight_sum = 0
            
        pass

class Generator (object):
    indicator_cnt = 0
    indicator_relationship_cnt = 0
    indicators = None 
    indicator_relationships = None 
    id_ = None
    
    def __init__(self):
        self.id_ = str(self).split("at ")[-1][:-2]
        self.indicators = {}
        self.indicator_relationships = {}


    def get_indicator(self, indicator):
        indicator_id = None

        if Indicator ==  type(indicator):
            indicator_id = indicator.id_

        return self.indicators[indicator_id]

    def get_indicators(self, indicators):
        indicators_r = []
        for indicator in indicators:
            if Indicator ==  type(indicator):
                if indicator.generator_id == self.id_:
                    indicators_r.append(self.indicators[indicator.id_])
                else:
                    raise IndexError("different generator id")
            else:
                indicators_r.append(self.indicators[indicator])
        return indicators_r




    def add_indicator(self, indicator, one_to_one=False):
        if not isinstance(indicator, Indicator):
            raise TypeError("Passed value '{}' is not Indicator type".format(indicator))
        if indicator.generator_id == self.id_ and self.id_:
            raise ValueError("FIx this part")
        else:
            self.indicator_cnt+=1
            self.indicators[self.indicator_cnt] = copy.deepcopy(indicator)

            row_id = self.indicator_cnt
            self.indicators[row_id].generator_id == self.id_
            self.indicators[row_id].id_ = row_id 
            print "ind: {}".format(self.indicators[row_id])
            if one_to_one:
                self.create_relationship(None,  self.indicators[row_id])

        return self.indicators[self.indicator_cnt] 


    def create_indicator(self, indicator_func, weight, one_to_one=False):
        self.indicator_cnt+=1
        indicator = Indicator(indicator_func, weight)

        indicator.id_ = self.indicator_cnt 
        indicator.generator_id = self.id_
        self.indicators[self.indicator_cnt] = indicator

        if one_to_one:
            self.create_relationship(None, self.indicators[self.indicator_cnt])
        return indicator


    def create_relationship(self, relationship_func, *indicators):
        print "indicatorssss: {}".format(indicators)
        for ind in indicators:
            if ind.generator_id is None or ind.id_ is None:
                self.add_indicator(ind)
        self.indicator_relationship_cnt+=1
        ir = IndicatorRelationship.__generator_init__(
            relationship_func,
            self.id_, self.indicator_relationship_cnt,
            *indicators
        )
        print 'ir_id: : {}'.format(ir)

        self.indicator_relationships[ir.id_] = ir#copy.deepcopy(ir)
        #come here
        print "ALL RELATIONSHIP  {}".format(self.indicator_relationships)

        return self.indicator_relationships[ir.id_]

    def add_relationship(self, ir):
        if ir.generator_id == self.id_:
            if not ir.id_ in self.indicator_relationships.keys():
                #in the future allow this by automatically rewriting to dict
                raise IndexError("Index is not in system, either deleted or corrupted")

        else:
            self.indicator_relationship_cnt+=1
            ir.set_generator_id(self.id_)
            ir.set_id(self.indicator_relationship_cnt)
       
        self.indicator_relationships[ir.id_] = copy.deepcopy(ir)
        return self.indicator_relationships[ir.id_]



    def get_indicator_check_funcs(self, ids):
        funcs = []
        for id_ in ids:
            func = self.indicators[id_].check_func
            funcs.append(func)
        return funcs

    def check(self, items):
        weight_sum = 0 
        for ir_id, ir in self.indicator_relationships.items():
            #ir = self.indicators[ir_id]
            if ir.one_to_one:
                indicator_id = ir.get_all_ids()[0]
                w = self.indicators[indicator_id].check(items)
            else:
                indicators = self.get_indicators(ir.get_all_ids())
                if not indicators:
                    raise ValueError("No indicators")
                w = ir.check_func(items, *indicators) * ir.multiplier
                print 'w: {}'.format(w)
            if (w):
                weight_sum+=w
        return weight_sum
 
        
class Indicator (object):
    enum = None
    weight = 0
    check_func = None
    type_ = 'Indicator'
    id_ = None
    generator_id = None

    def __init__(self, check_func, weight):
        self.check_func = check_func
        self.weight = weight

    def check(self, items):
        if not self.check_func:
            raise ValueError("Indicator.check_func is not set")
        weight_return = 0
        if self.check_func(items):
            weight_return = self.weight

        return weight_return







def below_10(values):
    print "BELOW_10"
    for value in values:
        if value < 10:
            return True 
    return False

def is_even(values):
    print "IS_EVEN"
    for value in values:
        if value % 2==0:
            return True
    return False

def and_(values, support, resistance):
    v = support.check_func(values) 
    f = resistance.check_func(values)
    print "AND_ {}".format(v and f)

    if v and f:
        return support.weight + resistance.weight
    return 0 

if __name__ == "__main__":
    pass
    '''
    g = Generator()
    i1 = Indicator(below_10, 5)
    i1 = g.add_indicator(i1)

    #i1 = g.create_indicator(below_10, 5)
    print '-------------------------\n\n'



    #i2 = Indicator(is_even, 9)
    #i2 = g.add_indicator(i2)


    i2 = g.create_indicator(is_even, 5)
    print '----------------------------\n\n'

    r1 = g.create_relationship(and_, i1, i2)
    r1.set_multiplier(1)
    values = [12, 1]
print 'generated weight: {}'.format(g.check(values))
'''
