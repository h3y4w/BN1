from binance.client import Client
from binance.enums import *

import pprint

api_key = '5l8vaf195WYJ8alCLP68eyUCFpHDTPdqb23vkPY7mmEhG8kEGCUZaROyf2G3FkQV'
api_secret = '6gF2FdqPPWRZg8Maqb7B3KbrbapuyYLCXg9Yo959YF6pTNcQ1fpzNvaJV65WAZgt'

client = Client(api_key, api_secret)

from binance.websockets import BinanceSocketManager

def process_message(msg):
    print(msg)
    print '---'

    # do something
'''
bm = BinanceSocketManager(client)
# start any sockets here, i.e a trade socket

#conn_key = bm.start_depth_socket('BNBBTC', process_message, depth=BinanceSocketManager.WEBSOCKET_DEPTH_5)
conn_key = bm.start_multiplex_socket(['bnbbtc@depth5','xrpbnb@depth5'], process_message)
# then start the socket manager
idx = conn_key.find('=')
keys = conn_key[idx+1:].split('/')
print keys
bm.start()
#bm.stop_socket('sds')#close with conn_key
#bm.close()#close all

'''
bnb_balance = client.get_asset_balance(asset='BNB')
print bnb_balance
if bnb_balance:
    bal = float(bnb_balance['free'])
    if bal > 1:
        #if first of pair is BNB then invert SIDE (ie from buy to sell)
        order = client.create_order( 
            symbol='BNBBTC',
            side=SIDE_SELL,
            type=ORDER_TYPE_LIMIT,
            timeInForce=TIME_IN_FORCE_GTC,
            quantity=1,
            price='0.00377'
        )
        print order

orders = client.get_all_orders(symbol='BNBBTC')
pprint.pprint(orders)
