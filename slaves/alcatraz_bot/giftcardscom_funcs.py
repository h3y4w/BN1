
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
GIFTCARDSCOM_PREPAID_VISA_LISTING = 'https://www.giftcards.com/visa-egift-gift-card'


def goto_prepaid_visa_listing(driver):
    driver.get(GIFTCARDSCOM_PREPAID_VISA_LISTING)


def input_prepaid_visa_value(driver, value, delay=10):
    #value_input = driver.find_element_by_id('denominationInput')
    elm = (By.ID, 'denominationInput')
    value_input = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm)) 
    value_input.click()
    value_input.clear()
    value_input.send_keys(value)


def input_prepaid_visa_fullname(driver, fullname, delay=10):
    #name_input = driver.find_element_by_id('name')
    elm = (By.ID, 'name')
    name_input = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))

    name_input.click()
    name_input.send_keys(fullname)


def input_prepaid_visa_email(driver, email, delay=10):
    #email_input = driver.find_element_by_id('recipient_email')
    elm = (By.ID, 'recipient_email')
    email_input = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))


    email_input.click()
    email_input.clear()
    email_input.send_keys(email)

    #confirm_email_input = driver.find_element_by_id('confirm_recipient_email')
    elm = (By.ID, 'confirm_recipient_email')
    confirm_email_input = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))


    confirm_email_input.click()
    confirm_email_input.clear()
    confirm_email_input.send_keys(email)

def input_prepaid_visa_message(driver, message, delay=10):
    #message_input = driver.find_element_by_id("message")

    elm = (By.ID, 'message')
    message_input = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))

    message_input.click()
    message_input.send_keys(message)

def input_prepaid_visa_next_btn(driver, delay=10):
    #next_btn = driver.find_element_by_class_name("btn.btn-primary")

    elm = (By.CLASS_NAME, 'btn.btn-primary')
    next_btn = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))

    next_btn.click()

def input_prepaid_visa_addtocart_btn(driver, delay=10):
    #addtocart_btn = driver.find_element_by_class_name("btn-primary.btn-block")
    elm = (By.CLASS_NAME, 'btn-block.btn-primary')
    addtocart_btn = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))
    addtocart_btn.click()

def input_prepaid_visa_checkout_btn(driver, delay=10):
    elm = (By.ID, 'nextOrProceedBtn')
    checkout_btn = WebDriverWait(driver, delay).until(EC.presence_of_element_located(elm))
    checkout_btn.click()
