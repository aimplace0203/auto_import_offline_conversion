import os
import re
import csv
import json
import datetime
import requests
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

def getLatestDownloadedFileName(downloadsDirPath):
    if len(os.listdir(downloadsDirPath)) == 0:
        return None
    return max (
        [downloadsDirPath + '/' + f for f in os.listdir(downloadsDirPath)],
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

def createCsvFile(data, outputFilePath):
    header = ["YCLID","コンバージョン名","コンバージョン発生日時","1コンバージョンあたりの価値","通貨コード"]
    with open(outputFilePath, 'w', newline='', encoding='cp932') as f:
        writer = csv.writer(f, delimiter=',', lineterminator='\r\n',  quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        writer.writerows(data)

def getAccessToken():
    try:
        url_token = f'https://biz-oauth.yahoo.co.jp/oauth/v1/token?grant_type=refresh_token' \
                f'&client_id={os.environ["YAHOO_CLIENT_ID"]}' \
                f'&client_secret={os.environ["YAHOO_CLIENT_SECRET"]}' \
                f'&refresh_token={os.environ["YAHOO_REFRESH_TOKEN"]}'
        req = requests.get(url_token)
        body = json.loads(req.text)
        access_token = body['access_token']
        return access_token
    except Exception as err:
        logger.error(f'Error: getAccessToken: {err}')
        exit(1)

def sendChatworkNotification(message):
    try:
        url = f'https://api.chatwork.com/v2/rooms/{os.environ["CHATWORK_ROOM_ID"]}/messages'
        headers = { 'X-ChatWorkToken': os.environ["CHATWORK_API_TOKEN"] }
        params = { 'body': message }
        requests.post(url, headers=headers, params=params)
    except Exception as err:
        logger.error(f'Error: sendChatworkNotification: {err}')
        exit(1)

def uploadCsvFile(data, outputFileName, outputFilePath):
    try:
        url_api = 'https://ads-search.yahooapis.jp/api/v6/OfflineConversionService/upload'
        headers = { 'Authorization': f'Bearer {getAccessToken()}' }
        files = { 'file': open(outputFilePath, mode='rb') }
        params = {
                'accountId': os.environ["YAHOO_ACCOUNT_ID"],
                'uploadType': 'NEW',
                'uploadFileName': outputFileName
                }
        req = requests.post(url_api, headers=headers, files=files, params=params)
        body = json.loads(req.text)
        logger.info(f'uploadCsvFile - status_code: {req.status_code}')
        logger.info(f'uploadCsvFile - response:\n--> {req.text}')

        if req.status_code != 200:
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += 'インポートに失敗しました。\n'
            message += '担当者は実行ログの確認を行ってください。\n\n'
            message += f'ステータスコード：{req.status_code}\n\n'
            message += f'昨日の発生件数は {len(data)} 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)

        if body['errors'] != None:
            errors = body['errors'][0]
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += 'インポートに失敗しました。\n'
            message += '担当者は実行ログの確認を行ってください。\n\n'
            message += f'ステータスコード：{req.status_code}\n'
            message += f'エラーコード：{errors["code"]}\n'
            message += f'エラーメッセージ：{errors["message"]}\n'
            message += f'エラー詳細：{errors["details"]}\n\n'
            message += f'昨日の発生件数は {len(data)} 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)
        
        return body['rval']['values'][0]['offlineConversion']['uploadId']
    except Exception as err:
        logger.debug(f'Error: uploadCsvFile: {err}')
        exit(1)

def checkUploadStatus(uploadId):
    try:
        url_api = f'https://ads-search.yahooapis.jp/api/v6/OfflineConversionService/get'
        headers = { 'Authorization': f'Bearer {getAccessToken()}' }
        params = {
                'accountId': os.environ["YAHOO_ACCOUNT_ID"],
                'uploadIds': [ uploadId ]
               }
        req = requests.post(url_api, headers=headers, json=params)
        body = json.loads(req.text)
        logger.info(f'checkUploadStatus - status_code: {req.status_code}')
        logger.info(f'checkUploadStatus - response:\n--> {req.text}')

        if req.status_code != 200:
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += 'インポート結果の取得に失敗しました。\n'
            message += '担当者はYahoo!広告管理画面からインポート結果の確認を行ってください。\n\n'
            message += f'ステータスコード：{req.status_code}\n\n'
            message += f'昨日の発生件数は {len(data)} 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)

        if body['errors'] != None:
            errors = body['errors'][0]
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += 'インポート結果の取得に失敗しました。\n'
            message += '担当者はYahoo!広告管理画面からインポート結果の確認を行ってください。\n\n'
            message += f'ステータスコード：{req.status_code}\n'
            message += f'エラーコード：{errors["code"]}\n'
            message += f'エラーメッセージ：{errors["message"]}\n'
            message += f'エラー詳細：{errors["details"]}\n\n'
            message += f'昨日の発生件数は {len(data)} 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)

        result = body['rval']['values'][0]['offlineConversion']
        message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
        message += 'インポートが完了しました。\n\n'
        message += f'アップロードID：{result["uploadId"]}\n'
        message += f'アップロード日時：{result["uploadedDate"]}\n'
        message += f'ステータス：{result["processStatus"]}\n\n'
        message += f'昨日の発生件数は {len(data)} 件です。[/info]'
        sendChatworkNotification(message)

    except Exception as err:
        logger.debug(f'Error: checkUploadStatus: {err}')
        exit(1)

### main_script ###
if __name__ == '__main__':

    try:
        downloadsDirPath = './csv'
        os.makedirs(downloadsDirPath, exist_ok=True)
        outputDirPath = './output'
        outputFileName = '育毛剤YSS_CV戻し.csv'
        os.makedirs(outputDirPath, exist_ok=True)
        outputFilePath = f'{outputDirPath}/{outputFileName}'

        logger.debug("import_yahoo: start get_domain_info")
        importCsvFromYahoo(downloadsDirPath)
        csvPath = getLatestDownloadedFileName(downloadsDirPath)
        logger.info(f"import_yahoo: download {csvPath}")

        data = list(getCsvData(csvPath))
        logger.info(data)
        if len(data) == 0:
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += '昨日の発生件数は 0 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)

        logger.info("import_yahoo: createCsvFile")
        createCsvFile(data, outputFilePath)

        logger.info("import_yahoo: uploadCsvFile")
        uploadId = uploadCsvFile(data, outputFileName, outputFilePath)
        sleep(30)

        logger.info(f"import_yahoo: uploadId -> {uploadId}")
        logger.info("import_yahoo: checkUploadStatus")
        checkUploadStatus(uploadId)
        logger.info("import_yahoo: Finish")
        exit(0)
    except Exception as err:
        logger.debug(f'import_yahoo: {err}')
        exit(1)
