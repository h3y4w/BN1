# -*- coding: utf-8 -*-
import json
import traceback
import os
from drivers.slavedriver import SlaveDriver
import time
from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from selenium import webdriver
from datetime import datetime
from dateutil.parser import parse
from urlparse import urlparse

import trade_functions
import indicators
import binance_indicators
from binance.client import Client
from binance.websockets import BinanceSocketManager


class BinanceTrader(SlaveDriver):
    model_id = 'BTV1'
    bm = None
    client = None
    def __init__(self, config):
        super(BinanceTrader, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@BTV1.echo.job': self.echo,
            'bd.sd.@BTV1.do.shit': self.do_shit,
            'bd.sd.@BTV1.done.shit': self.done_shit,

            'bd.sd.@BTV1.binance.socket.run': self.bd_sd_BTV1_binance_socket_run,
            'bd.sd.@BTV1.helloworld': self.bd_sd_BTV1_echo_job,
            'bd.sd.@BTV1.trade.swing': self.bd_sd_BTV1_trade_swing,
            'bd.sd.@BTV1.trade.swing.trader': self.bd_sd_BTV1_trade_swing_trader,
            'bd.sd.@BTV1.get.models': self.bd_sd_BTV1_get_models
        })
            #self.driver.init_warehouse_db()


    def binance_stream_event_handler(self, event):
        stream = event['stream']
        data = event['data']
        data['stream']=event['stream']
        if stream:
            stream = stream.replace('@', '/')
            #add watchdog here #track threads
            self.taskrunner_send_data_to_tag('bse+'+stream, data)

    def bd_sd_BTV1_echo_job(self, data, route_meta):
       time.sleep(5)
       print data
       print "FINISHED ECHO_JOB"

    def bd_sd_BTV1_binance_socket_stop(self, data, route_meta):
        raise NotImplemented
        #delete this shit

    def binance_socket_stop(self):
        print "Stopping Binance Socket Manager"
        if self.bm:
            self.bm.close()

    def binance_socket_run(self, api_key, api_secret, streams):
        print "Launching Binance Socket Manager"
        if self.bm:
            self.bm.close()

        self.client = Client(api_key, api_secret)
        if not self.client:
            raise ValueError("Incorrect Binance API credentials")

        self.bm = BinanceSocketManager(self.client)

        conn_keys = self.bm.start_multiplex_socket(streams, self.binance_stream_event_handler)
        idx = conn_keys.find('=')
        keys = conn_keys[idx+1:].split('/')

        if keys:

            self.bm.start()
            return keys

        else:
            self.binance_socket_stop()
            raise ValueError("Invalid Binance socket streams")

    def bd_sd_BTV1_binance_socket_run(self, data, route_meta): #issue happens running this function
        raise NotImplemented

    def bd_sd_BTV1_trade_swing_trader(self, data, route_meta):
        kline_g = indicators.Generator()
        depth_g = indicators.Generator()

        k3s = binance_indicators.Kline_3_soldiers()
        k3c = binance_indicators.Kline_3_crows()
        kh1 = binance_indicators.Kline_hammer()
        khi = binance_indicators.Kline_hammer_inverted()

        #kline_g.add_indicator(k3s, one_to_one=True)

        k3c = kline_g.add_indicator(k3c)
        khi = kline_g.add_indicator(khi)

        #ir = kline_g.create_relationship(binance_indicators.possibleUptrend, k3c, khi)

        #kline_g.add_indicator(kh1, one_to_one=True)

        depth_g.add_indicator(binance_indicators.FutureDemand(), one_to_one=True)


        conn_keys = self.binance_socket_run(data['api_key'], data['api_secret'], data['streams'])
        stream_tags = []

        if conn_keys:
            for key in conn_keys:
                k = key.replace('@', '/')
                stream_tag = 'bse+' + k 
                stream_tags.append(stream_tag)
        else:
            raise ValueError("Incorrect Binance streams provided: {}".format(data['streams']))



        def calculate_weighted_average(items, ranking=True, ):
            all_weight = 0
            avg = 0
            all_ranking_weight = 0
            for item in items:
                all_weight += item['weight']

            all_ranking_weight = float(all_weight * 0)
            all_weight += all_ranking_weight
            cnt = len(items)

            brackets = [.33, .33, .33]
            bracket_cnt = len(brackets)
            bracket_group_cnt = int(cnt / bracket_cnt)
            overflow =  cnt % bracket_cnt

            bracket_i=0
            last_bracket = False

            for i in xrange(0, cnt):
                item = items[i]

                if bracket_i == bracket_cnt-1:
                    last_bracket = True

                group_cnt = bracket_group_cnt
                if last_bracket and overflow:
                    group_cnt += overflow

                bracket_weight = brackets[bracket_i] * all_ranking_weight
                weight = bracket_weight/group_cnt
                weight += item['weight']

                avg = avg + item['value'] * float(weight/all_weight)

                if i > cnt *  (bracket_i+1/bracket_cnt): #gets correct weight for bracket
                    bracket_i = bracket_i + 1

            return avg


        prev_avg = 0
        last_c = None
        ticker = None
        trading_pair_data = {}
        stream_tags_data = {} 


        TICKER_PAST_MAX_LENGTH = 60
        DEPTH_PAST_MAX_LENGTH = 60

        while 1:
            for tag in stream_tags:
                pair, pair_route = trade_functions.extract_tag_info(tag)
                if not pair in stream_tags_data.keys():
                    stream_tags_data[pair] = {}

                if not pair_route in stream_tags_data[pair].keys():
                    func = None
                    max_length = 60
                    obj_key = None
                    if 'kline' in pair_route:
                        max_length = 25
                        func = kline_g.check
                        obj_key= 'k'
                    elif 'depth' in pair_route:
                        func = depth_g.check

                    elif 'ticker' in pair_route:
                        def printLastPrice (td):
                            print 'Last price: {}'.format(td[0]['c'])
                        func = printLastPrice

                    stream_tags_data[pair][pair_route] = trade_functions.StreamTagData(pair, pair_route, cb_func=func, max_length=max_length, obj_key=obj_key) 


                tag_data = self.taskrunner_inbox_get_tag(tag, get_all=True)
                updated = False
                YUUPP = None
                for td in tag_data:
                    updated = True
                    stream_tags_data[pair][pair_route].insert(td)

                if updated:
                    streams = stream_tags_data[pair].keys()
                    w = stream_tags_data[pair][pair_route].cb()
                    if w:
                        if w==69:
                            YUUPP = stream_tags_data[pair][pair_route].current


                    if YUUPP:
                        print "YUUPP: {}".format(YUUPP)
                    #for s in streams:
                    #    print '{} len: {}'.format(s, len(stream_tags_data[pair][s].all()))
                    #print '----------\n'
                    #if 'kline' in pair_route:
                    #    ms = ''
                    #    for td in stream_tags_data[pair][pair_route].all():
                    #        ms += str(td['k']['t']) + '\n'
                    #    print ms+'times^'
                    #    pass




    def bd_sd_BTV1_trade_swing(self, data, route_meta):
        data = {'pairs': ['bnbbtc'], 'depth': 5, 'kline_interval': '1m'}

        streams = []
        for pair in data['pairs']:
            streams.append(pair+'@depth'+str(data['depth'])) #stream for depth data
            streams.append(pair+'@kline_'+str(data['kline_interval']))
            streams.append(pair+'@ticker') #stream for ticker data

        query_tag = self.taskrunner_create_address_tag()

        q = 'select * from BinanceApiKey where write == 1 and read == 1 LIMIT 1;'
        self.query_WCV1(q,query_tag, name='Quering db for Binance api security credentials') 

        tdata = self.taskrunner_inbox_get_tag(query_tag, poll=10, delay=1)
        queried = None
        if not tdata:
            raise ValueError("Binance Api credentials could not be queried from database")

        queried = tdata['queried']
        if not queried:
            raise ValueError("No available Binance Api credentials with permissions")

        task_data = {}
        task_data['api_key'] = queried[0]['key']
        task_data['api_secret'] = queried[0]['secret']
        task_data['streams'] = streams

        self.add_global_task('bd.sd.@BTV1.trade.swing.trader', task_data, 'Swing Trading: {}'.format(streams))


    def bd_sd_BTV1_get_models(self, data, route_meta):
        tag = self.taskrunner_inbox_create_tag(route_meta['task_id'])
        self.query_WCV1(data['query'], tag) #query creates a task for WCV1

        tdata = self.taskrunner_inbox_get_tag(tag, 3, 1)
        if tdata:
            print tdata
            print "========================\n\n\n"

        else:
            print "\n\n\nDidnt get response fast enough\n\n\n"


    def do_shit(self, data, route_meta):
        cols = data.keys()

        tag = self.taskrunner_create_address_tag()
        ps = []
        for x in xrange(0, 10):
            p = {}
            p['invested_amount'] = x*38.31#data['a']

            if 'update' in cols:
                p['reference_id'] = data['id']
            ps.append(p)
        
        self.insert_WCV1('RCB_tmp', ps, route_meta['job_id'], tag)
        #d = self.taskrunner_inbox_get_tag(tag, poll=10, delay=1)

    def done_shit(self, data, route_meta):
        print 'DONE SHIT BITCH'


    def echo(self, data, route_meta):
        r = 'bd.sd.@BTV1.do.shit'
        d={'tasks':[{'a':3},{'a':34},{'a':6}],'cb_route':'bd.sd.@BTV1.done.shit'}
        self.add_global_task_group(r, d, 'Exctracting and inserting values', route_meta['job_id'])
        time.sleep(1)
        print "1.",
        time.sleep(1)
        print "2.",
        time.sleep(1)
        print "3."
        print 'AccessPoint.echos > payload: {} & returned: {}'.format(data, d)



