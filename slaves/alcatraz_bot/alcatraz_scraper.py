from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import time
import requests
import json


from functools import wraps

def select_click_option_value(select, value):
    options = select.find_elements_by_tag_name('option')
    flag = False
    for option in options:
        if option.get_attribute('value') == value:
            flag = True
            option.click()
            break
    if not flag:
        raise ValueError("Value '{}' was not found in select".format(value))

def payment_stage_scope(func):
    def wrap_and_call(*args, **kwargs):
        args[0].get_payment_stage()
        return func(*args, **kwargs)
    return wrap_and_call

def billing_stage_scope(func):
    def wrap_and_call(*args, **kwargs):
        args[0].get_billing_stage()
        return func(*args, **kwargs)
    return wrap_and_call

def ticket_stage_scope(func):
    def wrap_and_call(*args, **kwargs):
        args[0].get_ticket_stage()
        return func(*args, **kwargs)
    return wrap_and_call

def root_iframe_scope(func):
    def wrap_and_call(*args, **kwargs):
        args[0].refresh_root_iframe()
        return func(*args, **kwargs)
    return wrap_and_call

def payment_chase_iframe_scope(func):
    def wrap_and_call(*args, **kwargs):
    #    args[0].refresh_root_iframe()
        args[0].refresh_payment_chase_iframe()
        return func(*args, **kwargs)
    return wrap_and_call


class AlcatrazScraper (object):
    driver = None

    def __init__(self, driver):
        self.driver = driver
        pass

    def refresh_root_iframe(self):
        self.driver.switch_to.default_content()
        iframe = self.get_root_iframe()
        self.driver.switch_to.frame(iframe)

    def refresh_payment_chase_iframe(self):
        self.refresh_root_iframe()
        self.get_payment_stage()
        chase = self.get_payment_chase_iframe()
        self.driver.switch_to.frame(chase)

    @root_iframe_scope
    def get_ticket_stage(self):
        print 'Searching for Ticket Stage Element'
        elm = (By.ID, 'step-for-stage-ticketSelection')
        self.ticket_stage = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located(elm))
        return self.ticket_stage

    @root_iframe_scope
    def get_billing_stage(self):
        print 'Searching for Billing Stage Element'
        elm = (By.ID, 'step-for-stage-billing')
        self.billing_stage = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located(elm))
        return self.billing_stage
   
    
    @root_iframe_scope
    def get_payment_stage(self):
        print 'Searching for Payment Stage Element'
        elm = (By.ID, 'step-for-stage-processingPayment')
        self.payment_stage = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located(elm))
        return self.payment_stage

    @root_iframe_scope
    def get_confirmation_stage(self):
        print 'Searching for Confirmation Stage Element'
        elm = (By.ID, 'step-for-stage-confirmation')
        self.confirmation_stage = WebbDriverWait(self.driver, 30).until(EC.presence_of_element_located(elm))
        return self.confirmation_stage

    def get_payment_chase_iframe(self):
        payment_stage = self.get_payment_stage()
        print 'Getting chase iframe...'
        elm = (By.NAME, 'chaseHostedPayment')
        self.payment_chase_iframe = WebDriverWait(payment_stage, 30).until(EC.presence_of_element_located(elm))
        return self.payment_chase_iframe

    def get_root_iframe(self, delay=30):
        elm = (By.CLASS_NAME, 'zoid-visible')
        print 'Looking for iframe'
        self.root_iframe = WebDriverWait(self.driver, delay).until(EC.presence_of_element_located(elm))
        return self.root_iframe

    @billing_stage_scope
    def billing_get_form(self):
        elm = (By.ID, 'billing')
        form = WebDriverWait(self.billing_stage, 15).until(EC.presence_of_element_located(elm))
        return form 

    @billing_stage_scope
    def billing_set_name(self, first_name, last_name):
        first_input = self.billing_get_form().find_element_by_name('firstName')
        first_input.clear()
        first_input.click()
        first_input.send_keys(first_name)

        last_input = self.billing_get_form().find_element_by_name('lastName')
        last_input.clear()
        last_input.click()
        last_input.send_keys(last_name)
    
    @billing_stage_scope
    def billing_set_email(self, email):
        email_input = self.billing_get_form().find_element_by_name('email')
        email_input.clear()
        email_input.click()
        email_input.send_keys(email)

    @billing_stage_scope
    def billing_set_phone_number(self, phone_number):
        phone_input = self.billing_get_form().find_element_by_name('phone')
        phone_input.clear()
        phone_input.click()
        phone_input.send_keys(phone_number)
    
    @billing_stage_scope
    def billing_set_address_street(self, street):
        street_input = self.billing_get_form().find_element_by_name('address')
        street_input.clear()
        street_input.click()
        street_input.send_keys(street)

    @billing_stage_scope
    def billing_set_address_zipcode(self, zipcode):
        zipcode_input = self.billing_get_form().find_element_by_name('postalCode')
        zipcode_input.clear()
        zipcode_input.click()
        zipcode_input.send_keys(zipcode)

    @billing_stage_scope
    def billing_set_address_city(self, city):
        city_input = self.billing_get_form().find_element_by_name('city')
        city_input.clear()
        city_input.click()
        city_input.send_keys(city)

    @billing_stage_scope
    def billing_set_address_state(self, state):
        state_slot_select = self.billing_get_form().find_element_by_name('state')
        select_click_option_value(state_slot_select, state)


    @billing_stage_scope
    def billing_click_pay_btn(self):
        btn_group = self.billing_stage.find_element_by_id('payButtonIdUSD').find_element_by_xpath('./..')
        btn = btn_group.find_element_by_tag_name('button')
        btn.click()

    def ticket_stage(self, prospect):
        prospect_date = datetime.strptime(prospect['date'], '%m/%d/%Y')
        self.ticket_navigate_calendar_month(prospect_date)
        days = self.ticket_stage.find_elements_by_class_name('CalendarDay')

        flag = False
        for day in days:
            label = day.get_attribute('aria-label')
            if not 'Not available' in label:
                if 'Selected.' in label:
                    label = label.split('Selected. ')[1]
                #label format = Saturday, August 24, 2019
                cday = datetime.strptime(label, '%A, %B %d, %Y')
                if cday == prospect_date:
                    print 'Buy for this day: {}'.format(prospect['date'])
                    flag = True
                    day.click()
                    time.sleep(.5)
                    time_ = datetime.strptime(prospect['times'][0]['time'], '%H:%M:%S')
                    time_ = time_.strftime("%-I:%M %p")
                    self.ticket_set_ticket_time(time_)
                    self.ticket_set_adult_tickets(1)
                    self.ticket_click_continue_btn()
                    break
        if not flag:
            raise ValueError("Couldn't buy ticket for this day: {}".format(prospect['date']))


    def billing_stage(self):
        self.billing_set_name('Denise', 'Bitchass')
        self.billing_set_email('bitchass@gmail.com')
        self.billing_set_phone_number('4156320429')
        self.billing_set_address_street('1300 Turk st')

        self.billing_set_address_zipcode('94400')
        self.billing_set_address_city('San Francisco')
        self.billing_set_address_state('CA')
        self.billing_click_pay_btn()

    def payment_stage(self):
        card_info={'type':'Visa', 'cc':'457437867837', 'cvc':'786', 'exp':datetime(2021, 8, 1)}
        self.payment_set_cc_number(card_info['cc'])
        self.payment_set_cvc_number(card_info['cvc'])
        self.payment_set_card_type(card_info['type'])
        self.payment_set_exp_date(card_info['exp'])
        #payment_get_form(driver).submit()


    @ticket_stage_scope
    def ticket_click_continue_btn(self, delay=20):
        elm = (By.CSS_SELECTOR, "button[data-bdd='continue-button']")
        btn = WebDriverWait(self.ticket_stage, delay).until(EC.element_to_be_clickable(elm))
        btn.click()

    @ticket_stage_scope
    def ticket_set_adult_tickets(self, cnt, delay=15):
        elm = (By.TAG_NAME, 'input')
        cnt_input = WebDriverWait(self.ticket_stage, delay).until(EC.presence_of_element_located(elm))
        root = cnt_input.find_element_by_xpath('./..')
        cnt_inputs = self.ticket_stage.find_elements_by_tag_name('input')
        cnt_input = cnt_inputs[0]
        self.driver.execute_script("arguments[0].removeAttribute('readonly')", cnt_input)
        if cnt_input.get_attribute('readonly'):
            raise Exception("Could not input ticket cnt")

        cnt_input.click()
        cnt_input.clear()
        cnt_input.send_keys('1')

    @ticket_stage_scope
    def ticket_get_calendar_month (self):
        calendars = self.ticket_stage.find_elements_by_class_name("CalendarMonth")
        captions = self.ticket_stage.find_elements_by_class_name('CalendarMonth_caption')
        current = None
        for i, c in enumerate(calendars):
            if c.get_attribute('data-visible') == 'true':
                month = captions[i].get_attribute('innerHTML')
                try:
                    month = month.split('<strong>')[1].split('</strong>')[0]
                    current =  datetime.strptime(month, "%B %Y")
                    break
                except:
                    raise Exception("Month caption formatting changed")
        if current:
            return current
        raise Exception("No months are visible")

    @ticket_stage_scope
    def ticket_navigate_calendar_month(self, date):
        left_btn, right_btn = self.ticket_stage.find_elements_by_class_name('DayPickerNavigation_button')
        if date.day != 1:
            date = datetime(date.year, date.month, 1)

        current = self.ticket_get_calendar_month()
        print 'Current Calendar Month: {}'.format(current.month)

        while not (current == date):
            print 'tryna find it for: '+datetime.strftime(date, '%m/%d/%Y')
            print 'current: ' +datetime.strftime(current, '%m/%d/%Y')
            print '------'
            #switches calendar to correct month
            if current < date:
                print 'Going forward'
                right_btn.click()
            elif current > date:
                print 'Going back'
                left_btn.click()
            else:
                print "SET TO MONTH: {}".format(current.month)
                break
            self.ticket_navigate_calendar_month(date)
            time.sleep(.5)
            current = self.ticket_get_calendar_month()

    @ticket_stage_scope
    def ticket_set_ticket_time(self, time_):
        elm = (By.ID, 'availableTimeSlot')
        time_slot_input = WebDriverWait(self.driver, 15).until(EC.presence_of_element_located(elm))

        pm_am = None
        if 'PM' in time_:
            pm_am = 'PM'

        elif 'AM' in time_:
            pm_am = 'AM'

        if not pm_am:
            raise ValueError("Incorrect time format for input slot: {}".format(time_))

        pm_am_idx = time_.find(pm_am)
        if pm_am_idx>0:
            if time_[pm_am_idx-1] != ' ':
                time_ = time_[0:pm_am_idx-1]+' '+pm_am
         
        print "time: {}".format(time_)
        options = time_slot_input.find_elements_by_tag_name('option')
        for option in options:
            if time_ in option.text:
                option.click()
                break

    def run(self):
        self.driver = webdriver.Chrome(executable_path='/usr/local/share/chromedriver')
        pass

        pass



    
    @payment_chase_iframe_scope
    def payment_get_form(self):
        elm = (By.ID, 'theForm')
        try:
            return WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(elm))
        except Exception as e:
            print self.driver.find_element_by_xpath("*").get_attribute('innerHTML')
            print "ERROR: {}".format(e)
            raise e


    @payment_chase_iframe_scope
    def payment_set_cc_number(self, cc_number):
        payment_form = self.payment_get_form()

        cc_input = payment_form.find_element_by_id('ccNumber')
        cc_input.clear()
        cc_input.click()
        cc_input.send_keys(cc_number)

    @payment_chase_iframe_scope
    def payment_set_cvc_number(self, cvc_number):
        payment_form = self.payment_get_form()

        cvc_input = payment_form.find_element_by_id('CVV2')
        cvc_input.clear()
        cvc_input.click()
        cvc_input.send_keys(cvc_number)

    @payment_chase_iframe_scope
    def payment_set_card_type(self, card_type):

        payment_form = self.payment_get_form()
        card_type_select = payment_form.find_element_by_id('ccType')
        select_click_option_value(card_type_select, card_type)

    @payment_chase_iframe_scope
    def payment_set_exp_date(self, dt):
        payment_form = self.payment_get_form()

        month = dt.strftime('%m')
        year = dt.strftime('%Y')

        month_select = payment_form.find_element_by_id('expMonth')
        year_select = payment_form.find_element_by_id('expYear')

        select_click_option_value(month_select, month)
        select_click_option_value(year_select, year)

