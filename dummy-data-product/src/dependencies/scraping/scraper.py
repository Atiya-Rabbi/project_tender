import json
import logging
logging.basicConfig(level=logging.INFO)
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from  pyvirtualdisplay import Display
import os,re,time, gzip, shutil
import metadata_parser
from webdriver_manager.chrome import ChromeDriverManager
from dependencies.utils.utils import remove_prevfiles
from configurations.helper import read_config

class ProjectsTenders:
    def __init__(self) -> None:
        config = read_config()
        self.data_csv   = config['CSVFiles']['raw']
        self.meta_data  = config['JSONFiles']['meta_data']
        self.url        = config['URLs']['scrape_url']
        self.script_dir = config['GZip']['gzip_folder']

    def setup(self) -> str:
        '''Initialize Virtual Display and Selenium Webdriver'''
        #virtual display
        self.display = Display(visible=0, size=(800, 800))
        self.display.start()

        #download directory preferance
        prefs = {
            "download.default_directory"  :self.script_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade"  : True,
            "safebrowsing.enabled"        : False,
            "safebrowsing_for_trusted_sources_enabled": False,
        }
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs",prefs)
        '''
            better way would be installing chromedriver and specifying the path here
            but this is done to make it easy for testing
        ''' 
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),chrome_options=chrome_options)
        wait = WebDriverWait(self.driver, 10)
        self.driver.get(self.url)
        get_url = self.driver.current_url
        wait.until(EC.url_to_be(self.url))
        logging.info("Selenium Webdriver-> {}".format(get_url))
        #sleep for 5sec so that complete page loads
        time.sleep(5)
        return get_url

    def scrape_maindata(self,get_url) -> None:
        '''Find Download CSV button on website'''
        flag = True
        if get_url == self.url:
            try:
                xpaths = [
                    '//button[@title="Download data as gzipped JSON"]',
                    '//*[@id="app"]/search/search_tender/div[3]/div/tender-table/div[2]/div/button'
                ]
                for xpath in xpaths:
                    download_btn = self.driver.find_element(by=By.XPATH, value=xpath)
                    if download_btn:
                        logging.info('Download Button Found')
                        break
            except Exception as e:
                flag = False
                logging.exception(e)
            
            if download_btn:
                flag = True
                download_btn.click()
                time.sleep(1)
                xpath = '//button[@title="Download data as gzipped CSV"]'
                self.driver.find_element(by=By.XPATH, value=xpath).click()
                logging.info('Downloading at location: {}'.format(self.script_dir))
            
            if flag:
                for _ in range(3):
                    if self.check_downloaded_file():
                        break
                    logging.info('Downloading...')
                    time.sleep(300)
                
            else:
                logging.error('Something went wrong...')    

    def close_scraper(self) -> None:        
        '''Close Webdriver and Stop Virtual Display'''
        self.driver.close()
        self.display.stop()

    def check_downloaded_file(self) -> bool:
        '''Check if zip file is downloaded then extract it'''

        downloaded_file = [f for f in os.listdir(self.script_dir) if f.endswith(".gz")]
        if len(downloaded_file) !=0:
            try:
                with gzip.open(self.script_dir+'/'+downloaded_file[0], 'rb') as f_in:
                    with open(self.data_csv, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        logging.info('Extracted CSV file: {}'.format(self.data_csv))
                return True
            except Exception as e:
                logging.exception(e)
                return False

    def scrape_metadata(self) -> None:
        '''Scrape MetaData'''
        
        remove_prevfiles(file_name=self.meta_data)
        page = metadata_parser.MetadataParser(self.url)
        with open(self.meta_data, 'w') as f_out:
            f_out.write(json.dumps(page.metadata))
            f_out.close()
            logging.info('Metadata: {}'.format(self.meta_data))

    def run_scraper(self) -> None:
        '''Scraper Runner'''
        #remove previous zipfile and csv file
        remove_prevfiles(zipfile=self.script_dir,file_name=self.data_csv)
        
        #setup for scraping via selenium
        get_url = self.setup()

        #scrape main func
        self.scrape_maindata(get_url)

        #close scraper
        self.close_scraper()

        #remove zipfile
        remove_prevfiles(zipfile=self.script_dir)
        