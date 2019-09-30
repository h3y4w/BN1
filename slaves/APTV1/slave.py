# -*- coding: utf-8 -*-
import json
import traceback
import os
from drivers.scraper_slavedriver import ScraperSlaveDriver
import time
from utils.mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from dateutil.parser import parse
from urlparse import urlparse


class APTV1(ScraperSlaveDriver):
    model_id = 'APTV1'
    bm = None
    client = None

    BROWSENODES_LIST_URL = "https://www.browsenodes.com/amazon.com"
    BROWSENODES_CHILDREN_URL = "https://www.browsenodes.com/amazon.com/browseNodeLookup/{}.html"


    def __init__(self, config):
        super(APTV1, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@APTV1.run': self.bd_sd_APTV1_run,
            'bd.sd.@APTV1.scrape.product.pages': self.bd_sd_APTV1_scrape_product_pages,
            'bd.sd.@APTV1.scrape.amazon.nodes': self.bd_sd_APTV1_scrape_amazon_nodes,
            'bd.sd.@APTV1.scrape.amazon.top.nodes': self.bd_sd_APTV1_scrape_amazon_top_nodes,
            'bd.sd.@APTV1.merge.amazon.nodes': self.bd_sd_APTV1_merge_amazon_nodes,
            'bd.sd.@APTV1.get.amazon.nodes': self.bd_sd_APTV1_get_amazon_nodes,
            'bd.sd.@APTV1.test': self.test123,
            'bd.sd.@APTV1.test123': self.test

        })


    def test(self, data, route_meta):
        pass

    def test123(self, data, route_meta):
        cnt = 5
        if 'cnt' in data.keys():
            cnt = data['cnt']

        for i in xrange(0, cnt):
            self.add_global_task (
                'bd.sd.@APTV1.test123',
                {'cnt': cnt-1},
                "cnt: {} i:{}".format(cnt-1, i)
            )

        
    def bd_sd_APTV1_scrape_categories(self, data, route_meta):
        pass

    def bd_sd_APTV1_scrape_category_pages(self, data, route_meta):
        pass

    def read_product_information(self, rows):
        product = {}
        for row in rows:
            header = row.find_element_by_tag_name('th')
            data = row.find_element_by_tag_name('td')

            if header.text == 'ASIN':
                product['ASIN'] = data.text

            elif header.text == 'Product Dimensions':
                product['dimensions'] = data.text

            elif header.text == 'Item Weight':
                product['item_weight'] = data.text

            elif header.text == 'Shipping Weight':
                product['shipping_weight'] = data.text

                product['item_model_number'] = data.text

        return product
    
    def read_product_details(self, divs):
        details = {}
        for div in divs:
            label = div.find_element_by_tag_name('label')
            data = div.find_element_by_tag_name('span')
            if label:
                label = label.text.split(':')[0]
                details[label] = data.text
        return details


    def bd_sd_APTV1_scrape_product_pages(self, data, route_meta):
#        product_pages = ["https://www.amazon.com/Jessie-Classic-Child-Costume-Small/dp/B01LXX98Q8/"]

        product_pages = ['https://www.amazon.com/Disney-Princess-Little-Mermaid-Ariel/dp/B00BHO5VPC']
#        product_pages = ['https://www.amazon.com/Disney-Deluxe-Descendants-Costume-X-Large/dp/B01NAZ8BLU']
        driver = self.create_driver()
        products = []
        for url in product_pages:
            product = {}
            driver.get(url)
            center_desc = driver.find_element_by_id('centerCol')

            table = driver.find_element_by_id('productDetails_detailBullets_sections1')
            tbody = table.find_element_by_tag_name('tbody')
            rows = tbody.find_elements_by_tag_name('tr')
            product_info = self.read_product_information(rows)


            form = center_desc.find_element_by_id('twister')
            divs = form.find_elements_by_tag_name('div')
            product_details = self.read_product_details(divs)


            product['name']=center_desc.find_element_by_id('title_feature_div').text
            product['price']=center_desc.find_element_by_id('priceblock_ourprice').text

            product['info'] = product_info
            product['detail'] = product_details


                
            products.append(product)
        print products




    def scrape_amazon_node_page(self, table):
        nodes = []
        for row in table.find_elements_by_tag_name('tr'):
            node = {}
            cols = row.find_elements_by_tag_name('td')
            if len(cols) > 1:
                node['name'] = cols[0].text
                node['id'] = int(cols[1].text)
                nodes.append(node)
        return nodes

    def get_browsenodes_table(self, driver, url):
        driver.get(url)
        table = driver.find_element_by_tag_name('tbody')
        return table

    def get_browsenodes_nodes(self, table):
        nodes = self.scrape_amazon_node_page(table)
        return nodes

    def get_browsenodes(self, driver, node_id=None):
        url = self.BROWSENODES_LIST_URL
        if node_id:
            url = self.BROWSENODES_CHILDREN_URL.format(node_id)

        table = self.get_browsenodes_table(driver, url)
        nodes = self.get_browsenodes_nodes(table)
        return nodes


    def get_browsenodes_all(self, driver, node_id=None):
        nodes = [{'id':node_id}]
        if not node_id:
            nodes = self.get_browsenodes(driver)
        print "STARTING NODES: {}".format(nodes)
        length = len(nodes)
        for i in xrange(0, length):
            node = nodes[i]
            new_nodes = self.get_browsenodes(driver, node_id=node['id'])
            print "NEW NODES FROM {}: {}".format(node['id'], new_nodes)
            node['has_children'] = False

            if len(new_nodes):
                node['has_children'] = True 
                nodes+=new_nodes
                length = len(nodes)

        return nodes


    def bd_sd_APTV1_check_amazon_nodes(self, data, route_meta):
        tag = self.taskrunner_create_address_tag()

    def bd_sd_APTV1_merge_amazon_nodes(self, data, route_meta):
        #FUTURE MAKE IT SO THAT IT RECEIVE WAIT AND ADD TAG DATA AS IT GOES
        node_ids = []
        for node in data['nodes']: 
            node_ids.append(node['id'])

        updated_nodes = []
        query = "select * from AmazonCategoryNode;"
        tag = self.taskrunner_create_address_tag()
        self.query_WCV1(query, tag) #query creates a task for WCV1
        tdata = self.taskrunner_inbox_get_tag(tag, 20, .5, raise_error=True, err_msg='Warehouse db call for amazon nodes timed out')
        queried = tdata['queried']
        scraped_nodes = data['nodes']

        if queried:
            for node in queried:
                if node['category_node_id'] in node_ids:
                    length = len(scraped_nodes)
                    for i in xrange(0, length):

                        new_node = scraped_nodes[i]
                        
                        if new_node['id'] == node['category_node_id']:
                            UPDATE_NODE = False
                            HAS_CHILDREN_INFO = False

                            if not 'name' in new_node.keys():
                                new_node['name'] = node['category_name']
                                HAS_CHILDREN_INFO = True
                                if node['has_children'] != new_node['has_children']:
                                        UPDATE_NODE = True

                            if node['category_name'] !=  new_node['name']:
                                UPDATE_NODE = True

                            if UPDATE_NODE:
                                n = {
                                    'category_name': new_node['name'],
                                    'category_node_id': node['category_node_id'],
                                    'id': node['id'],
                                }
                                if HAS_CHILDREN_INFO:
                                    n['has_children'] = new_node['has_children']

                                updated_nodes.append(n)
                           
                            del scraped_nodes[i] 
                            length-=1

                            if i !=0:
                                i-=1
                            break
                            
        self.insert_WCV1('AmazonCategoryNode',scraped_nodes)  
        if len(updated_nodes):
            self.update_WCV1('AmazonCategoryNode',updated_nodes)

    def bd_sd_APTV1_scrape_amazon_top_nodes(self, data, route_meta):
        driver = self.create_driver()
        top_nodes = self.get_browsenodes(driver)
        driver.quit()

        self.add_global_task (
            'bd.sd.@APTV1.merge.amazon.nodes',
            {'nodes': top_nodes},
            "Merging {} categories into warehouse db".format(len(top_nodes))
        )

        self.add_global_task (
            'bd.sd.@APTV1.scrape.amazon.nodes',
            {'node_id': top_nodes[0]['id']},
            "Searching for subcategories in '{}'".format(top_nodes[0]['name'])
        )

        if 0:
            for top in top_nodes:
                self.add_global_task (
                    'bd.sd.@APTV1.scrape.amazon.nodes',
                    {'node_id': top['id']},
                    "Searching for subcategories in '{}'".format(top['name'])
                )

    def bd_sd_APTV1_scrape_amazon_nodes(self, data, route_meta):
        keys = data.keys()
        driver = self.create_driver()
        nodes = []
        node_id = None
        if 'node_id' in keys:
            node_id = data['node_id']

        nodes = self.get_browsenodes_all(driver, node_id=node_id)

        length = len(nodes)

        if length:
            self.add_global_task (
                'bd.sd.@APTV1.merge.amazon.nodes',
                {'nodes': nodes},
                "Merging {} categories into warehouse db".format(length)
            )
        driver.quit()

#        self.taskrunner_send_data_to_tag(tag, nodes)



    def bd_sd_APTV1_get_amazon_nodes(self, data, route_meta):
        query = "select * from AmazonCategoryNode;"
        tag = self.taskrunner_create_address_tag()
        self.query_WCV1(query, tag) #query creates a task for WCV1

        tdata = self.taskrunner_inbox_get_tag(tag, 20, .5)
        if tdata:
            print tdata
            print "========================\n\n\n"

        else:
            print "\n\n\nDidnt get response fast enough\n\n\n"


    def bd_sd_APTV1_run(self, data, route_meta):
        #this gets all urls for menu categories
        driver = self.create_driver()
        driver.get('https://amazon.com')
        menu_btn = driver.find_element_by_id('nav-hamburger-menu')
        menu_btn.click()
        time.sleep(5)
        menu = driver.find_elements_by_class_name('hmenu-visible')[1]
        categories = menu.find_elements_by_tag_name('li')
        pull_category = False
        found_start = False
        pulled_categories = []
        print "length: {}".format(len(categories))

        for i, category in enumerate(categories):
            try:
                row = category.find_element_by_class_name('hmenu-item')
                classes = row.get_attribute('class').split(" ")
                if row.tag_name == 'a':
                    if not found_start:
                        div = row.find_element_by_tag_name('div')
                        if 'Clothing' in div.text:
                            found_start = True
                        else:
                            continue
                    pulled_categories.append(category)

                for pulled in pulled_categories:
                    print pulled.find_element_by_tag_name('div').text()
            except Exception as e:
                #noelementexception
                classes = category.get_attribute('class').split(' ')
                if 'hmenu-link-separator' in classes and found_start:
                    print 'FOUND LINK SEPERATOR PROP BREAKING'
                    break
                else:
                    raise e

        pulled_categories[0].click() 


