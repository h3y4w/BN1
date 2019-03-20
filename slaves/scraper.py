# -*- coding: utf-8 -*-
import json
import traceback
import os
from bot import SlaveDriver, OUTBOX_TASK_MSG
from selenium import webdriver
import time
from datetime import datetime
from mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG


class Scraper(SlaveDriver):
    model_id = 'SCV1'
    def __init__(self, config):
        super(Scraper, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@SCV1.echo': self.echo,
            'bd.sd.@SCV1.#echos': self.echos,
            'bd.sd.@SCV1.scrape.#balsta': self.task_scrape_balsta,

            'bd.sd.@SCV1.^job1': self.job1,
            'bd.sd.@SCV1.#job1_a': self.job1_a,
            'bd.sd.@SCV1.*taskgroup': self.taskgroup 
        })
            #self.driver.init_warehouse_db()

    def job1 (self, data, route_meta):
        self.add_global_task('bd.sd.@SCV1.#job1_a', {}, 'Big job1 test', job_id=route_meta['job_id'])

        self.send_msg(create_local_task_message('bd.@md.slave.job.completed', {'job_id':route_meta['job_id'], 'msg': 'Sent!'}), 1)

    def job1_a(self, data, route_meta):
        data = {
            "tasks": [
                {"msg": 1}, {"msg": 2}, {"msg":3}
            ]
        }
        msg = self.add_global_task_group('bd.sd.@SCV1.*taskgroup', data, 'TaskGroup Test', route_meta['job_id'])

    def taskgroup(self, data, route_meta):
        print '\n------{}--------\n'.format(data)

    def echos(self, data, route_meta):
        time.sleep(1)
        print "1.",
        time.sleep(1)
        print "2.",
        time.sleep(1)
        print "3."
        print 'AccessPoint.echos > payload: {}'.format(data)

    def echo(self, payload):
        print 'Scraper.echo > payload: {}'.format(payload)
        a = []
        a[4]

    def __scrape_balsta__getBids (self, args):
        driver = webdriver.Firefox()
        items_bids = {}
        urls = args['urls']
        for url in urls:
            driver.get(url)
            information_class = driver.find_element_by_class_name('information')
            item_id = information_class.find_element_by_class_name('productId').text
            print "ITEM ID: {}".format(item_id)
            #driver.find_element_by_class_name('buttons').get_attribute('button').click()
            popup = driver.find_element_by_id('infoContainer')
            driver.execute_script("""
                var element = arguments[0];
                element.parentNode.removeChild(element);
                """, popup)

            driver.find_element_by_class_name('showHideBidHistory').click() #show bid history
            time.sleep(.5)
            bid_history = driver.find_element_by_id('bidHistory').find_elements_by_css_selector('li')

            bids  = []
            if len(bid_history) > 2: #bid_history has 1 row if there are no bids (the row tells us there are no bids)
                security = bid_history.pop(0)
                security_reached = security.find_element_by_class_name('above').text == u'Bevakningspris uppnått'

                for bid in bid_history:
                    bid_info = {}
                    ts = int(bid.get_attribute('data-bid-created'))
                    
                    dt = datetime.utcfromtimestamp(ts)#
                    bid_info['time'] = dt

                    bidder_info = bid.find_element_by_class_name('bidder').text
                    idx = bidder_info.find('(')+1
                    idx1 = bidder_info.find(' ', idx)
                    price = bidder_info[idx:idx1]

                    bid_info['price'] = price 
                    bids.append(bid_info)
                items_bids[item_id] = bids
        print "ITEMS_BIDS: {}".format(items_bids)

    def __scrape_balsta__extraItemList (self, item_list):
        items_info = [] 
        urls = []
        for item in item_list:
            item_info = {}
            information_class = item.find_element_by_class_name('information')
            bid_info = information_class.find_element_by_class_name('productBidCurrent').find_element_by_class_name('currentBid').text
            end_info = information_class.find_element_by_class_name('productClock').get_attribute('data-time')
            urls.append(information_class.find_element_by_tag_name('a').get_attribute('href'))

            #price = ''.join(bid_info.split()[0:-1])
            currency = bid_info.split(' ')[-1]

            item_info['id'] = int(item.get_attribute('data-product-id'))
            #item_info['price'] = float(price)
            item_info['currency'] = currency
            item_info['end_time'] = int(end_info)
            item_info['bids'] = []
            items_info.append(item_info)

        return items_info, urls

    def task_scrape_balsta(self, args):
        try:
            url = 'https://balstaauktionshall.nu/objektlista/guld-silver--smycken?page={}'
            driver = webdriver.Firefox()
            items_info = [] 
            page_num = 1
            entries = 0
            expected_total = -1

            urls = []
            while (expected_total != entries):
                driver.get(url.format(page_num))
                expected_total = int(driver.find_element_by_class_name('entriesTotal').text.split()[0])
                product_list = driver.find_element_by_id('productList')
                item_list = product_list.find_elements_by_css_selector('li')
                more_items_info, more_urls = self.__scrape_balsta__extraItemList(item_list)
                items_info += more_items_info 
                urls += more_urls
                entries = len(items_info)
                page_num+=1
                break
            driver.quit()
            #print items_info#, entries, expected_total
            #if  split work between scraper slaves:
                #do this

        except Exception as e:
            print "ERROR: {}".format(traceback.print_exc())
            #self.driver.task_error(args, str(e))
        else:
            pass


    def job_scrape_balsta(self, args):
        print args
        pass
if __name__ == "__main__":
    Scraper().start()
