import os
from bot import Master, Slave
import time
import gspread
from selenium import webdriver

class Sheeter(Slave):
    model_id = 'SHV1'
    def __init__(self, driver):
        payload = {}
        self.driver = driver
        super(Scraper, self).__init__(payload)
        self.add_command_mappings({
        })

        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']

        self.credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/den0/Downloads/sn-millie-3c71edcede81.json', scope)
        self.gc = gspread.authorize(credentials)

    def cmd_gsheet_add(self, args):
        pass

    def cmd_gsheet_remove(self, args):
        self.gc.del_spreadsheet(args['spreadsheet_id'])

    def cmd_scrape_balsta(self, args):
        try:
            url = 'https://balstaauktionshall.nu/objektlista/guld-silver--smycken?page={}'
            driver = webdriver.Firefox()
            items_info = [] 
            page_num = 1
            entries = 0
            expected_total = -1

            while (expected_total != entries):
                driver.get(url.format(page_num))
                expected_total = int(driver.find_element_by_class_name('entriesTotal').text.split()[0])
                time.sleep(3)
                product_list = driver.find_element_by_id('productList')
                item_list = product_list.find_elements_by_css_selector('li')
                items_info += self.scrape_balsta__extraItemList(item_list)
                entries = len(items_info)
                page_num+=1
            driver.quit()
            print "\n\nCALCULATED..."
            print items_info, entries, expected_total
        except Exception as e:
            self.driver.task_error(args, str(e))
        else:
            self.driver.task_completed(args)
        finally:
            self.finished()

