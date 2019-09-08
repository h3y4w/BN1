from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import time
import requests
import json

ALCATRAZCRUISES_DAY_TOUR_URL = 'https://www.alcatrazcruises.com/checkout/?id=1000016'

def createTicketTimeInfoRequestBody(date):
    return {
       "operationName":"ticketAvailability",
       "variables": {
           "propertyId": "hac",
           "bookingType":1000016,
           "date":date,
           "correlationId":"d64d50b5-3ef1-435d-aed1-b8fedb714ac8",
           "withTourResources":False
       },
       "query":"query ticketAvailability($propertyId: String!, $bookingType: String!, $date: String!, $correlationId: String!, $costRateId: Int, $token: String, $withTourResources: Boolean!) {\n  ticketAvailability(propertyId: $propertyId, bookingType: $bookingType, date: $date, correlationId: $correlationId, costRateId: $costRateId, token: $token) {\n    BookingTypeId\n    TimedTicketTypeId\n    StartDate\n    StartTime\n    EndTime\n    vacancy\n    pricing\n    ticketsData {\n      TicketId\n      TicketPrice\n      IsTaxInclusive\n      TaxPercentage\n      Taxes {\n        TaxName\n        TaxPercentage\n        TaxIncluded\n        __typename\n      }\n      TicketDescription\n      AvailableOnline\n      SortOrder\n      TourProductClass\n      TourProductSubclass\n      ProductTypes {\n        id\n        value\n        __typename\n      }\n      productInfo(propertyId: $propertyId) {\n        id\n        productId\n        trackingLabel\n        pairedProducts\n        pairedProductsMinQuantity\n        settings {\n          id\n          value\n          __typename\n        }\n        translations {\n          id\n          values {\n            id\n            value\n            __typename\n          }\n          __typename\n        }\n        media {\n          type\n          url\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    tourResources(propertyId: $propertyId, correlationId: $correlationId) @include(if: $withTourResources) {\n      ResourceName\n      __typename\n    }\n    __typename\n  }\n}\n"} 

def createTicketInfoRequestBody(startDate, endDate):
    return {
        "operationName":"initiateCheckoutQuery",
        "variables": {
            "propertyId":"hac",
            "bookingType":1000016,
            "startDate":startDate,
            "endDate":endDate,
            "showPrivateTours":False,
            "correlationId":"d64d50b5-3ef1-435d-aed1-b8fedb714ac8"
        },
        "query": "query initiateCheckoutQuery($propertyId: String!, $correlationId: String!, $bookingType: String, $wpPostId: String, $startDate: String!, $endDate: String!, $showPrivateTours: Boolean) {\n  propertyConfig(propertyId: $propertyId) {\n    id\n    isActive\n    name\n    propertyId\n    tealiumProfileName\n    theme\n    currencies {\n      id\n      isDefault\n      label\n      currencyId\n      __typename\n    }\n    tickets {\n      id\n      isInsurance\n      label\n      __typename\n    }\n    templates {\n      id\n      translations {\n        id\n        value\n        __typename\n      }\n      __typename\n    }\n    features {\n      id\n      value\n      __typename\n    }\n    settings {\n      id\n      value\n      __typename\n    }\n    foreignCurrency(correlationId: $correlationId)\n    __typename\n  }\n  searchTours(propertyId: $propertyId, bookingType: $bookingType, wpPostId: $wpPostId, showPrivateTours: $showPrivateTours) {\n    tourId\n    propertyId\n    wpPostId\n    bookingTypeId\n    permalink\n    localizedInfo {\n      locale\n      values {\n        id\n        value\n        __typename\n      }\n      __typename\n    }\n    settings {\n      id\n      value\n      __typename\n    }\n    media {\n      caption\n      sortOrder\n      type\n      url\n      __typename\n    }\n    enabledDates(startDate: $startDate, endDate: $endDate, correlationId: $correlationId) {\n      year\n      months {\n        month\n        days\n        availability {\n          availability\n          day\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    udfs {\n      UDFCategoryName\n      UDFCategoryType\n      UDFCategoryDescription\n      SortOrder\n      UDFs {\n        UDFName\n        UDFCategoryName\n        UDFCategoryType\n        UDFDescription\n        UDFType\n        UDFLength\n        IsRequired\n        AllowEdit\n        SortOrder\n        Picklist\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"}


def goto_alcatrazcruises_day_tour_listing(driver):
    driver.get(ALCATRAZCRUISES_DAY_TOUR_URL)


def post_hornblower_graphql(data):
    r = requests.post('https://my.hornblower.com/graphql', data=data)
    rdata = json.loads(r.text)
    resp_keys= rdata.keys()
    if 'errors' in resp_keys:
        raise Exception("Hornerblower graphql error: {}".format(rdata['errors']))
    return rdata

    
def get_ticket_time_availability(date):
    data = json.dumps(createTicketTimeInfoRequestBody(date))
    rdata = post_hornblower_graphql(data)
    return rdata['data']['ticketAvailability']

def get_ticket_availability(start, end):
    data = json.dumps(createTicketInfoRequestBody(start, end))
    rdata = post_hornblower_graphql(data)
    return rdata['data']['searchTours'][0]['enabledDates']

def selection_wait_for_iframe(driver, delay=30):
    elm = (By.XPATH, '//iframe')
    elm = (By.CLASS_NAME, 'zoid-visible')
    print 'Looking for iframe'
    return WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))

def ticket_selection_get_root_element(driver):
    elm = (By.XPATH, '/html/body/div')
    elm = (By.XPATH, '//*[contains(@name,"xcomponent__hb_commerce__latest__")]') 
           #return driver.find_element_by_xpath(elm[1])
    return WebDriverWait(driver, 10).until(EC.presence_of_element_located(elm))

def ticket_selection_get_stage_element(element, delay=15):
    elm = (By.ID, 'step-for-stage-ticketSelection')
    elm = (By.XPATH, '/html/body/div/div/div/div/div/div/div/div')
    ticket_stage = None
    print 'Searching for Ticket Stage Element'
    e = None
    #root_element().location_once_scrolled_into_view this is a property
    search_delay = 20
    ##############
    #for i in xrange(0, 3):
    #    search_delay = delay
    #    try:
    return WebDriverWait(element, search_delay).until(EC.presence_of_element_located(elm))

def ticket_selection_access_calendar(driver):
    iframe = selection_wait_for_iframe(driver)
    return ticket_selection_get_stage_element(driver)


def start_purchase_process(driver, prospect, card_info={'type':'Visa', 'cc':'457437867837', 'cvc':'786', 'exp':datetime(2021, 8, 1)}):
    goto_alcatrazcruises_day_tour_listing(driver)
    time.sleep(15)
    iframe = selection_wait_for_iframe(driver, 60)
    driver.switch_to.frame(selection_wait_for_iframe(driver))
    ticket_stage = ticket_selection_get_stage_element(driver)

    prospect_date = datetime.strptime(prospect['date'], '%m/%d/%Y')
    ticket_selection_navigate_calendar_month(ticket_stage, prospect_date)
    days = ticket_stage.find_elements_by_class_name('CalendarDay')
    for day in days:
        label = day.get_attribute('aria-label')
        if not 'Not available' in label:
            if 'Selected.' in label:
                label = label.split('Selected. ')[1]
            #label format = Saturday, August 24, 2019
            cday = datetime.strptime(label, '%A, %B %d, %Y')
            if cday == prospect_date:
                print 'Buy for this day: {}'.format(prospect['date'])
                day.click()
                time.sleep(.5)
                time_ = datetime.strptime(prospect['times'][0]['time'], '%H:%M:%S')
                time_ = time_.strftime("%-I:%M %p")
                ticket_selection_set_ticket_time(ticket_stage, time_)
                ticket_selection_set_adult_tickets(driver, ticket_stage, 1)
                ticket_selection_click_continue_btn(ticket_stage)
                driver.switch_to.default_content()
                break

    iframe = selection_wait_for_iframe(driver, 60)
    driver.switch_to.frame(selection_wait_for_iframe(driver))

    billing_stage = billing_section_get_stage_element(driver)
    billing_section_set_name(billing_stage, 'Denise', 'Bitchass')
    billing_section_set_email(billing_stage, 'bitchass@gmail.com')
    billing_section_set_phone_number(billing_stage, '4156320429')
    billing_section_set_address_street(billing_stage, '1300 Turk st')
    billing_section_set_address_zipcode(billing_stage, '94400')
    billing_section_set_address_city(billing_stage, 'San Francisco')
    billing_section_set_address_state(billing_stage, 'CA')
    billing_section_click_pay_btn(billing_stage)

    SWITCHED_TO_CHASE_FRAME = False
    payment_stage = None

    for i in xrange(0, 5):
        driver.switch_to.default_content()

        driver.switch_to.frame(selection_wait_for_iframe(driver))

        payment_stage = payment_section_get_stage_element(driver)

        #37776
        #5353

        print 'Searching for iframes'
        flag = False
        frames = []
        for i in xrange(0, 10):
            frames = payment_stage.find_elements_by_tag_name('iframe')
            if (len(frames)>0):
                flag = True
                break
            time.sleep(1)

        if not flag:
            raise ValueError("No iframes could be detected on payment stage")
        print 'frames: {}'.format(len(frames))
        for frame in frames:
            print 'name: {}'.format(frame.get_attribute('name'))
        
        iframe = payment_section_get_chase_iframe(payment_stage)
        driver.switch_to.frame(iframe)
        e = None
        try:
            e = driver.find_element_by_xpath('/html/body')
        except:
            continue
        frame_len = len(e.get_attribute('innerHTML'))
        if frame_len <= 8000:
            SWITCHED_TO_CHASE_FRAME = True
            break

        time.sleep(1)

    if not SWITCHED_TO_CHASE_FRAME:
        raise ValueError("Could not successfully switch to chase frame")

    payment_form = None

    try:
        payment_form = payment_get_form(driver)
    except:
        raise ValueError("Cant get payment form")


    payment_section_set_cc_number(driver, card_info['cc'])
    payment_section_set_cvc_number(driver, card_info['cvc'])
    payment_section_set_card_type(driver, card_info['type'])
    payment_section_set_exp_date(driver, card_info['exp'])
    #payment_get_form(driver).submit()
    time.sleep(5)


def ticket_selection_set_ticket_time(driver, time_):
    elm = (By.ID, 'availableTimeSlot')
    search_delay=15
    time_slot_input = WebDriverWait(driver, search_delay).until(EC.presence_of_element_located(elm))

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


def payment_section_get_stage_element(driver):
    elm = (By.XPATH, '/html/body/div/div/div/div/div/div/div[5]')
    print 'Searching for Payment Stage Element'
    search_delay = 30
    return WebDriverWait(driver, search_delay).until(EC.presence_of_element_located(elm))

def payment_section_get_chase_iframe(payment_stage):
    print 'Getting chase iframe...'
    elm = (By.NAME, 'chaseHostedPayment')
    return WebDriverWait(payment_stage, 30).until(EC.presence_of_element_located(elm))

def payment_get_form(driver):
    elm = (By.ID, 'theForm')
    #elm = (By.XPATH, '/html/body/form')
    return WebDriverWait(driver, 20).until(EC.presence_of_element_located(elm))
    
def payment_section_set_cc_number(driver, cc_number):
    payment_form = payment_get_form(driver)

    cc_input = payment_form.find_element_by_id('ccNumber')
    cc_input.clear()
    cc_input.click()
    cc_input.send_keys(cc_number)

def payment_section_set_cvc_number(driver, cvc_number):
    payment_form = payment_get_form(driver)

    cvc_input = payment_form.find_element_by_id('CVV2')
    cvc_input.clear()
    cvc_input.click()
    cvc_input.send_keys(cvc_number)

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

def payment_section_set_card_type(driver, card_type):

    payment_form = payment_get_form(driver)
    card_type_select = payment_form.find_element_by_id('ccType')
    select_click_option_value(card_type_select, card_type)


def payment_section_set_exp_date(driver, dt):
    payment_form = payment_get_form(driver)

    month = dt.strftime('%m')
    year = dt.strftime('%Y')

    month_select = payment_form.find_element_by_id('expMonth')
    year_select = payment_form.find_element_by_id('expYear')

    select_click_option_value(month_select, month)
    select_click_option_value(year_select, year)

def billing_section_get_stage_element(driver):
    elm = (By.XPATH, '/html/body/div/div/div/div/div/div/div[3]')
    print 'Searching for Billing Stage Element'
    search_delay = 30
    return WebDriverWait(driver, search_delay).until(EC.presence_of_element_located(elm))

def billing_get_form(billing_stage):
    elm = (By.ID, 'billing')
    billing = WebDriverWait(billing_stage, 15).until(EC.presence_of_element_located(elm))
    return billing

def billing_section_set_name(billing_stage, first_name, last_name):
    first_input = billing_get_form(billing_stage).find_element_by_name('firstName')
    first_input.clear()
    first_input.click()
    first_input.send_keys(first_name)

    last_input = billing_get_form(billing_stage).find_element_by_name('lastName')
    last_input.clear()
    last_input.click()
    last_input.send_keys(last_name)
    

def billing_section_set_email(billing_stage, email):
    email_input = billing_get_form(billing_stage).find_element_by_name('email')
    email_input.clear()
    email_input.click()
    email_input.send_keys(email)

def billing_section_set_phone_number(billing_stage, phone_number):
    phone_input = billing_get_form(billing_stage).find_element_by_name('phone')
    phone_input.clear()
    phone_input.click()
    phone_input.send_keys(phone_number)

def billing_section_set_address_street(billing_stage, street):
    street_input = billing_get_form(billing_stage).find_element_by_name('address')
    street_input.clear()
    street_input.click()
    street_input.send_keys(street)

def billing_section_set_address_zipcode(billing_stage, zipcode):
    zipcode_input = billing_get_form(billing_stage).find_element_by_name('postalCode')
    zipcode_input.clear()
    zipcode_input.click()
    zipcode_input.send_keys(zipcode)


def billing_section_set_address_city(billing_stage, city):
    city_input = billing_get_form(billing_stage).find_element_by_name('city')
    city_input.clear()
    city_input.click()
    city_input.send_keys(city)

def billing_section_set_address_state(billing_stage, state):
    state_slot_select = billing_get_form(billing_stage).find_element_by_name('state')
    select_click_option_value(state_slot_select, state)

def billing_section_click_pay_btn(billing_stage):
    btn_group = billing_stage.find_element_by_id('payButtonIdUSD').find_element_by_xpath('./..')
    btn = btn_group.find_element_by_tag_name('button')
    btn.click()

def ticket_selection_click_continue_btn(ticket_stage, delay=20):
    #(By.CSS_SELECTOR, '69-quantity-picker'
    elm = (By.CSS_SELECTOR, "button[data-bdd='continue-button']")
    btn = WebDriverWait(ticket_stage, delay).until(EC.element_to_be_clickable(elm))
    btn.click()

def ticket_selection_set_adult_tickets(driver, ticket_stage, cnt, delay=15):
    elm = (By.ID, '69-quantity-picker')
    elm = (By.TAG_NAME, 'input')
    #row = WebDriverWait(ticket_stage, delay).until(EC.presence_of_element_located(elm))

    #elm = (By.XPATH, '/div[1]/div/input')
    cnt_input = WebDriverWait(ticket_stage, delay).until(EC.presence_of_element_located(elm))
    root = cnt_input.find_element_by_xpath('./..')
    print root.get_attribute('innerHTML')
    cnt_inputs = ticket_stage.find_elements_by_tag_name('input')
    cnt_input = cnt_inputs[0]
    driver.execute_script("arguments[0].removeAttribute('readonly')", cnt_input)
    if cnt_input.get_attribute('readonly'):
        raise Exception("Could not input ticket cnt")

    cnt_input.click()
    cnt_input.clear()
    cnt_input.send_keys('1')


def ticket_selection_get_calendar_month (ticket_stage):
    calendars = ticket_stage.find_elements_by_class_name("CalendarMonth")
    captions = ticket_stage.find_elements_by_class_name('CalendarMonth_caption')
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

def ticket_selection_navigate_calendar_month(ticket_stage, date):
    left_btn, right_btn = ticket_stage.find_elements_by_class_name('DayPickerNavigation_button')
    if date.day != 1:
        date = datetime(date.year, date.month, 1)

    current = ticket_selection_get_calendar_month(ticket_stage)
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
        ticket_selection_navigate_calendar_month(ticket_stage, date)
        time.sleep(.5)
        current = ticket_selection_get_calendar_month(ticket_stage)


