# -*- coding: utf-8 -*-
import json
import traceback
import os
from drivers.slavedriver import SlaveDriver
import time
from utils.mpinbox import create_local_task_message, INBOX_SYS_MSG, INBOX_TASK1_MSG, OUTBOX_SYS_MSG, OUTBOX_TASK_MSG

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from dateutil.parser import parse
from urlparse import urlparse

from alcatraz_scraper import AlcatrazScraper


import giftcardscom_funcs
import alcatrazcruises_funcs

class AlcatrazTicketBot(SlaveDriver):
    model_id = 'ATBV1'
    bm = None
    client = None
    def __init__(self, config):
        super(AlcatrazTicketBot, self).__init__(config)
        self.add_command_mappings({
            'bd.sd.@ATBV1.echo.job': self.echo,
            'bd.sd.@ATBV1.$prepaid.buy': self.bd_sd_ATBV1_job_prepaid_buy,
            'bd.sd.@ATBV1.$alcatraz.tickets.manage': self.bd_sd_ATBV1_job_alcatraz_tickets_manage,
            'bd.sd.@ATBV1.$alcatraz.tickets': self.bd_sd_ATBV1_job_alcatraz_tickets,
            'bd.sd.@ATBV1.alcatraz.tickets.availability': self.bd_sd_ATBV1_alcatraz_tickets_availability,
            'bd.sd.@ATBV1.alcatraz.tickets.purchase': self.bd_sd_ATBV1_alcatraz_tickets_purchase
        })

    def bd_sd_ATBV1_alcatraz_tickets_availability(self, data, route_meta):


        now = datetime.utcnow()
        future = datetime.utcnow() + timedelta(days=31*3) #approx 3 months
        start = now.strftime('%m/%d/%Y')
        end = future.strftime('%m/%d/%Y')

        avail = alcatrazcruises_funcs.get_ticket_availability(start, end)
        availabilities = []
        for a in avail:
            year = 2000+a['year'] #returns last two values of uear (int)
            for m in a['months']:
                month = m['month']
                for d in m['availability']:
                    day = d['day']
            
                    dstr = '{}/{}/{}'.format(month, day, year)
                    date = datetime.strptime(dstr, "%m/%d/%Y")
                    obj = {}
                    obj = {'date': date, 'availability': d['availability'], 'times':[]}
                    add = None 
                    times = None
                    for pref in data['preferences']:

                        keys = pref.keys()
                        start = datetime.strptime(pref['start'], '%m/%d/%Y')
                        end = datetime.strptime(pref['end'], "%m/%d/%Y")
                        if not (date >= start and date <= end): 
                            if add == None:
                                add = False
                            #doesnt add day if its not range
                            continue

                        if 'weekdays' in keys:
                            #monday = 0, friday=4 .. etc
                            if not (date.weekday() in pref['weekdays']):
                                if add == None: #hasnt been set yet
                                    add = False
                                continue

                        #otherwise gets time info for day
                        if not times:
                            #gets time data
                            times = alcatrazcruises_funcs.get_ticket_time_availability(dstr)

                        for time in times:
                            if 'times' in keys:
                                for t_pref in pref['times']:
                                    if t_pref == time['StartTime']:
                                        if time['vacancy']>0:
                                            obj['times'].append({'time': time['StartTime'], 'availability': time['vacancy']})
                                            break
                            else:
                                if time['vacancy']>0:
                                    obj['times'].append({'time': time['StartTime'], 'availability': time['vacancy']})

                        add = True
                    if add:
                        obj['date'] = datetime.strftime(obj['date'], '%m/%d/%Y')
                        availabilities.append(obj)


        #in the future create a taskgroup for every time purchase
        rd = {'purchase_times':availabilities}
        print "availabilities: {}".format(availabilities)
        cnt = 0
        for i, time in enumerate(availabilities):
            if cnt == 3:
                break
            if time['times']:
                self.add_global_task('bd.sd.@ATBV1.alcatraz.tickets.purchase', {'purchase_time':time}, 'Purchasing tickets for {}'.format(time['date']))
                cnt+=1


    def bd_sd_ATBV1_alcatraz_tickets_purchase(self, data, route_meta):
        try:
            print '\n\n\n\nrd: {}'.format(data)
            options = Options()
            #options.add_argument("--headless")
            options.add_argument("--window-size=1920x1080")
            options.add_argument("--disable-gpu")
            def create_driver():
                driver = webdriver.Chrome(executable_path='/usr/local/share/chromedriver', chrome_options=options)

                driver.get('https://www.alcatrazcruises.com/checkout/?id=1000016')
                return driver

            driver = create_driver() 

            abot = AlcatrazScraper(driver)
            time.sleep(5)

            flag_level = 0
            error = None
            for i in xrange(0, 5):
                if flag_level == -1:
                    print "RELAUNCHING DRIVER"
                    driver.quit()
                    driver = create_driver()
                    abot = AlcatrazScraper(driver)
                    try:
                        abot.get_root_iframe()
                    except:
                        continue
                    else:
                        time.sleep(1)
                        flag_level = 0

                if flag_level == 0:
                    try:
                        print 'Trying ticket stage...'
                        abot.ticket_stage(data['purchase_time'])
                    except Exception as error:
                        flag_level = -1
                        continue
                    else:
                        time.sleep(2)
                        flag_level=1
                
                if flag_level == 1:
                    try:
                        abot.billing_stage() #user info
                    except Exception as error:
                        flag_level = -1
                        continue
                    else:
                        time.sleep(2)
                        flag_level=2

                if flag_level == 2:
                    try:
                        abot.payment_stage() #card info
                    except Exception as error:
                        flag_level = -1

                        continue
                    else:
                        flag_level=3
                        break

            if flag_level !=3:
                print '~~~~~~~~~~~~~~~~~~~~~~~~~~'
                print traceback.format_exc()
                raise Exception("Couldn't purchase ticket: {}".format(error))

        except Exception as e:
            print "ERROR\n-------------"
            print traceback.format_exc()
            raise e

        finally:
            driver.quit()

    def bd_sd_ATBV1_job_alcatraz_tickets(self, data, route_meta):
        d = {'preferences': [
            {
                'start': '08/20/2019',
                'end': '08/25/2019',
                'times': ['9:00:00', '11:00:00']
            },
            {
                'start': '08/25/2019', #make it so that if a date is in multiple preferences it incorporates other times
                'end': '08/30/2019',
                #'weekdays': [4,5,6], #weekends & friday
                'times': ['12:00:00']
            }
        ]}
        self.add_global_task('bd.sd.@ATBV1.alcatraz.tickets.availability', d, 'Check ticket availabilities')


    def bd_sd_ATBV1_job_prepaid_buy (self, data, route_meta):
        driver = webdriver.Firefox()
        giftcardscom_funcs.goto_prepaid_visa_listing(driver)
        giftcardscom_funcs.input_prepaid_visa_value(driver, 300)
        giftcardscom_funcs.input_prepaid_visa_fullname(driver, 'John Doe')
        giftcardscom_funcs.input_prepaid_visa_email(driver, "jd@gmail.com")
        giftcardscom_funcs.input_prepaid_visa_message(driver, "F*** u")
        giftcardscom_funcs.input_prepaid_visa_next_btn(driver)
        giftcardscom_funcs.input_prepaid_visa_addtocart_btn(driver, delay=20)
        giftcardscom_funcs.input_prepaid_visa_checkout_btn(driver, delay=20)
        time.sleep(8)
        driver.quit()



    def bd_sd_ATBV1_job_alcatraz_tickets_manage(self, data, route_meta):
        query_tag = self.taskrunner_create_address_tag()
        
        q= 'select * from TicketGroup where active=1 and ticket_drop_id is null;'
        #q = 'select * from TicketDropType;'
        self.query_WCV1(q,query_tag, name='Quering db for active and unallocated tickets') 

        tdata = self.taskrunner_inbox_get_tag(query_tag, poll=10, delay=1)
        queried = None
        if not tdata:
            raise ValueError("Binance Api credentials could not be queried from database")

        queried = tdata.get('queried')
        if not queried:
            raise ValueError("No available Binance Api credentials with permissions")

        print queried

    def echo(self, data, route_meta):
       time.sleep(5)
       print "FINISHED ECHO_JOB"

