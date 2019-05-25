# -*- coding: utf-8 -*-
import json
import traceback
import os
from bot import SlaveDriver, OUTBOX_TASK_MSG
import time
from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from selenium import webdriver
from datetime import datetime
from dateutil.parser import parse
from urlparse import urlparse

import sqmonitor_funcs

from binance.client import Client
from binance.websockets import BinanceSocketManager


class BinanceTrader(SlaveDriver):
    model_id = 'BTV1'
    bm = None
    client = None
    def __init__(self, config):
        super(BinanceTrader, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@BTV1.helloworld': self.echo,
            'bd.sd.@SCV1.#echos': self.echos,
            'bd.sd.@BTV1.binance.socket.run': self.bd_sd_BTV1_binance_socket_run,
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


        TICKER_PAST_MAX_LENGTH = 60
        DEPTH_PAST_MAX_LENGTH = 60

        while 1:
            for tag in stream_tags:
                if 'ticker' in tag:
                    ticker_data = self.taskrunner_inbox_get_tag(tag, get_all=True)
                    for trd in ticker_data:
                        pair = trd['stream'].split('@')[0]

                        if not pair in trading_pair_data.keys():
                            trading_pair_data[pair] = {'ticker': {'c': None, 'p': []}, 'depth': {'c': None, 'p':[]}} 

                        if not trading_pair_data[pair]['ticker']['c']:
                            trading_pair_data[pair]['ticker']['c'] = trd
                            continue

                        if trd['E'] > trading_pair_data[pair]['ticker']['c']['E']: #sets the most current ticker
                            old_trd = trading_pair_data[pair]['ticker']['c']
                            trading_pair_data[pair]['ticker']['c'] = trd
                            trd = old_trd #now its not the most currect trd so it will check to see where it will add in list

                        past = trading_pair_data[pair]['ticker']['p']
                        idx = 0
                        for idx in xrange(0, len(past)):
                            if past[idx]['E'] < trd['E']:
                                break

                        trading_pair_data[pair]['ticker']['p'].insert(idx, trd)

                        if TICKER_PAST_MAX_LENGTH < len(trading_pair_data[pair]['ticker']['p']):
                            trading_pair_data[pair]['ticker']['p'].pop()



                elif 'depth' in tag:
                    tags_data = self.taskrunner_inbox_get_tag(tag, get_all=True)
                    if tags_data:
                        for td in tags_data:
                            pair = td['stream'].split('@')[0]
                            if not pair in trading_pair_data.keys():
                                trading_pair_data[pair] = {'ticker': {'c': None, 'p': []}, 'depth': {'c': None, 'p':[]}} 

                            if not trading_pair_data[pair]['depth']['c']:
                                trading_pair_data[pair]['depth']['c'] = td
                                continue

                            if td['lastUpdateId'] > trading_pair_data[pair]['depth']['c']['lastUpdateId']: #sets the most current ticker
                                old_td = trading_pair_data[pair]['depth']['c']
                                trading_pair_data[pair]['depth']['c'] = td
                                td = old_td #now its not the most currect trd so it will check to see where it will add in list

                            past = trading_pair_data[pair]['depth']['p']
                            added = False
                            idx = 0
                            for idx in xrange(0, len(past)):
                                if past[idx]['lastUpdateId'] > td['lastUpdateId']:
                                    break

                            trading_pair_data[pair]['depth']['p'].insert(idx, td)

                            if DEPTH_PAST_MAX_LENGTH < len(trading_pair_data[pair]['depth']['p']):
                                trading_pair_data[pair]['depth']['p'].pop()


                            pair = td['stream'].split('@')[0]
                            if pair in trading_pair_data.keys():
                                td = trading_pair_data[pair]['depth']['c']
                                if td:
                                    bid_max_amount = 0 
                                    bid_max_amount_price = 0

                                    ask_max_amount = 0
                                    items = []
                                    for i in xrange(0, len(td['bids'])):
                                        bid = td['bids'][i]
                                        items.append({'weight': float(bid[1]), 'value': float(bid[0])})
                                        ask = td['asks'][i]
                                        items.append({'weight': float(ask[1]), 'value': float(ask[0])})

                                        if bid_max_amount < float(bid[1]):
                                            bid_max_amount = float(bid[1])
                                        if ask_max_amount < float(ask[1]):
                                            ask_max_amount = float(ask[1])

                                    bar = ''
                                    #for ask in td['asks']:
                                    for j in xrange(0, len(td['asks'])): #did it reverse  to have descendidng order
                                        k = (len(td['asks'])-1)  - j
                                        ask = td['asks'][k]
                                        bar = bar +  "{:.7f} = ".format(float(ask[0]), 7)
                                        for i in xrange(0, int( float(ask[1]) / ask_max_amount * 20)):
                                            bar = bar+'*'
                                        bar = bar+' ({})\n'.format(ask[1])
                            
                                    avg = calculate_weighted_average(items)
                                    mom = '-'
                                    if prev_avg:
                                        if prev_avg < avg:
                                            mom = 'I'
                                        if prev_avg > avg:
                                            mom = 'D'

                                    c = None
                                    diff = None
                                    pred = '-'
                                    if trading_pair_data[pair]['ticker']['c']:
                                        c = float(trading_pair_data[pair]['ticker']['c']['c'])
                                        if avg > float(c):
                                            pred = 'I'
                                        if avg < float(c):
                                            pred = 'D'
                                        if last_c:
                                            diff =  (1-last_c/c) * 100

                                    bar = bar +  "\n~~~~~~~~p: {} pred: {} mdir: {} diff: {} mavg: {:.7f}~~~~~~~~~~\n".format(c, pred, mom, diff,avg)

                                    for bid in td['bids']:
                                        bar = bar + "{:.7f} = ".format(round(float(bid[0]), 7))
                                        for i in xrange(0, int( float(bid[1]) / bid_max_amount * 25)):
                                            bar = bar+'*'
                                        bar = bar+ ' ({})\n'.format(bid[1])


               

                                    #if avg > trading_pair_data[pair]['ticker']['c']:
                                    raise NotImplemented("Add algo")

                                    trading_pair_data[pair]['ticker']['c']
                                    os.system("clear")
                                    print "update id: {}".format(td['lastUpdateId']) 
                                    print "======Depth============="
                                    print bar + '=----------------------='
                                    #print len(trading_pair_data[pair]['depth']['p'])
                                    #print trading_pair_data[pair]['depth']
                                    prev_avg = avg
                                    last_c = c



    def bd_sd_BTV1_trade_swing(self, data, route_meta):
        data = {'pairs': ['bnbbtc'], 'depth': 5}
        streams = []
        for pair in data['pairs']:
            streams.append(pair+'@depth'+str(data['depth'])) #stream for depth data
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


    def echo(self, data, route_meta):
        time.sleep(1)
        print "1.",
        time.sleep(1)
        print "2.",
        time.sleep(1)
        print "3."
        print 'AccessPoint.echos > payload: {}'.format(data)



