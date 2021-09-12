import os
import re
import csv
import datetime
import pywinauto
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager

# Logger setting
from logging import getLogger, FileHandler, DEBUG
logger = getLogger(__name__)
today = datetime.datetime.now()
os.makedirs('./log', exist_ok=True)
handler = FileHandler(f'log/{today.strftime("%Y-%m-%d")}_result.log', mode='a')
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False

### functions ###
def importCsvFromYahoo(downloadsFilePath):
    url = "https://www.afi-b.com/"
    login = os.environ['AFB_ID']
    password = os.environ['AFB_PASS']
    
    ua = UserAgent()
    logger.debug(f'importCsvFromYahoo: UserAgent: {ua.chrome}')

    options = Options()
    options.add_argument(f'user-agent={ua.chrome}')

    prefs = {
        "profile.default_content_settings.popups": 1,
        "download.default_directory": 
                os.path.abspath(downloadsFilePath),
        "directory_upgrade": True
    }
    options.add_experimental_option("prefs", prefs)
    
    try:
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        
        driver.get(url)
        driver.maximize_window()
        sleep(2)

        driver.find_element_by_xpath('//input[@name="login_name"]').send_keys(login)
        driver.find_element_by_xpath('//input[@name="password"]').send_keys(password)
        driver.find_element_by_xpath('//input[@type="submit"]').click()

        logger.debug('importCsvFromYahoo: afb login')
        sleep(5)
        
        driver.find_element_by_xpath('//a[@href="/pa/result/"]').click()
        sleep(2)
        driver.find_element_by_xpath('//a[@href="javascript:void(0)"]').click()
        sleep(2)
        driver.find_element_by_id('site_select_chzn_o_1').click()

        logger.info('importCsvFromYahoo: select site')
        sleep(2)

        driver.find_element_by_xpath('//input[@value="ytd"]').click()
        logger.info('importCsvFromYahoo: select date range')
        sleep(1)

        driver.find_element_by_xpath('//input[@src="/assets/img/report/btn_original_csv.gif"]').click()
        sleep(10)

        driver.close()
        driver.quit()
    except Exception as err:
        logger.debug(f'Error: importCsvFromYahoo: {err}')
        exit(1)

def getLatestDownloadedFileName(downloadsFilePath):
    if len(os.listdir(downloadsFilePath)) == 0:
        return None
    return max (
        [downloadsFilePath + '/' + f for f in os.listdir(downloadsFilePath)],
        key=os.path.getctime
    )

def getCsvData(csvPath):
    with open(csvPath, newline='', encoding='cp932') as csvfile:
        buf = csv.reader(csvfile, delimiter=',', lineterminator='\r\n', skipinitialspace=True)
        next(buf)
        for row in buf:
            index = row[16].find('yclid=')
            if index == -1:
                continue
            yclid = row[16].split('yclid=')[1]
            date = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            tdate = date.strftime('%Y%m%d %H%M%S Asia/Tokyo')
            yield [yclid, 'real_cv', tdate, row[9], 'JPY']

def createCsvFile(data):
    header = ["YCLID","コンバージョン名","コンバージョン発生日時","1コンバージョンあたりの価値","通貨コード"]
    with open('./output/育毛剤YSS_CV戻し.csv', 'w', newline='', encoding='cp932') as f:
        writer = csv.writer(f, delimiter=',', lineterminator='\r\n',  quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        writer.writerows(data)

def uploadCsvFile():
    url = "https://business.yahoo.co.jp/"
    login = os.environ['YAHOO_ID']
    password = os.environ['YAHOO_PASS']
    
    ua = UserAgent()
    logger.debug(f'uploadCsvFile: UserAgent: {ua.chrome}')

    options = Options()
    options.add_argument(f'user-agent={ua.chrome}')

    try:
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        
        driver.get(url)
        driver.maximize_window()
        driver.implicitly_wait(10)

        driver.find_element_by_link_text('ログイン').click()
        driver.implicitly_wait(20)

        driver.find_element_by_id('user_name').send_keys(login)
        driver.find_element_by_id('password').send_keys(password)
        driver.find_element_by_xpath('//input[@type="submit"]').click()

        logger.debug('uploadCsvFile: yahoo login')
        driver.implicitly_wait(20)
        
        driver.find_element_by_link_text('広告管理ツール').click()
        driver.implicitly_wait(20)
        driver.find_element_by_link_text('検索広告').click()
        driver.implicitly_wait(20)
        driver.find_element_by_link_text('ツール').click()
        driver.implicitly_wait(20)
        driver.find_element_by_link_text('コンバージョン測定').click()
        driver.implicitly_wait(20)
        driver.find_element_by_link_text('オフラインコンバージョンのインポート').click()
        driver.implicitly_wait(20)
        driver.find_element_by_xpath('//a[@data-test="showUploadPanel"]').click()
        driver.implicitly_wait(20)
        driver.find_element_by_xpath('//input[@data-test="file"]').click()

        findWindow = lambda: pywinauto.findwindows.find_windows(title='開く')[0]

        dialog = pywinauto.timings.wait_until_passes(5, 1, findWindow)
        pwa_app = pywinauto.Application()
        pwa_app.connect(handle=dialog)
        window = pwa_app['開く']
        window.wait('ready')

        pywinauto.keyboard.send_keys("%N")
        edit = window.Edit4
        edit.set_focus()
        edit.set_text(file_path)

        button = window['開く(&O):']
        button.click()

        #driver.find_element_by_xpath('//input[@data-text="submitButton"]').click()

        driver.close()
        driver.quit()
    except Exception as err:
        logger.debug(f'Error: uploadCsvFile: {err}')
        exit(1)

### main_script ###
if __name__ == '__main__':

    try:
        downloadsFilePath = './csv'
        os.makedirs(downloadsFilePath, exist_ok=True)

        logger.debug("import_yahoo: start get_domain_info")
        importCsvFromYahoo(downloadsFilePath)
        csvPath = getLatestDownloadedFileName(downloadsFilePath)
        logger.info(f"import_yahoo: download {csvPath}")

        data = list(getCsvData(csvPath))
        logger.info(data)
        createCsvFile(data)

        uploadCsvFile()
        logger.info("import_yahoo: Finish")
        exit(0)
    except Exception as err:
        logger.debug(f'import_yahoo: {err}')
        exit(1)
