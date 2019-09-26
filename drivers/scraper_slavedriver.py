from drivers.slavedriver import SlaveDriver

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class ScraperSlaveDriver (SlaveDriver):
    def __init__(self, config):
        super(ScraperSlaveDriver, self).__init__(config)

    def create_driver(self):
        options = Options()
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-gpu")
        exe_path = '/usr/lib/chromium-browser/chromedriver'

        #if self.is_ec2: #testing
        options.add_argument("--headless")

        return webdriver.Chrome(executable_path=exe_path, chrome_options=options)
