import os
import re
import csv
import sys
import json
import shutil
import datetime
import requests
import gspread
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from oauth2client.service_account import ServiceAccountCredentials

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
def importCsvFromAfb(downloadsDirPath, no, d):
    url = "https://www.afi-b.com/"
    login = os.environ['AFB_ID']
    password = os.environ['AFB_PASS']

    logger.debug(d)
    if d == 0:
        da = "td"
    elif d == 1:
        da = "ytd"
    
    ua = UserAgent()
    logger.debug(f'importCsvFromAfb: UserAgent: {ua.chrome}')

    options = Options()
    options.add_argument(f'user-agent={ua.chrome}')

    prefs = {
        "profile.default_content_settings.popups": 1,
        "download.default_directory": 
                os.path.abspath(downloadsDirPath),
        "directory_upgrade": True
    }
    options.add_experimental_option("prefs", prefs)
    
    try:
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
        
        driver.get(url)
        driver.maximize_window()
        driver.implicitly_wait(30)

        driver.find_element_by_xpath('//input[@name="login_name"]').send_keys(login)
        driver.find_element_by_xpath('//input[@name="password"]').send_keys(password)
        driver.find_element_by_xpath('//button[@type="submit"]').click()

        logger.debug('importCsvFromAfb: afb login')
        driver.implicitly_wait(60)
        
        driver.find_element_by_xpath('//a[@href="/pa/result/"]').click()
        driver.implicitly_wait(30)
        driver.find_element_by_xpath('//a[@href="javascript:void(0)"]').click()
        driver.implicitly_wait(30)
        select = driver.find_element_by_id(f'site_select_chzn_o_{no}')
        if not (re.search(r'822434', select.text) or re.search(r'806580', select.text)):
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += 'インポートに失敗しました。\n'
            message += 'AFBのCSV取込処理における対象サイトに不備があります。\n'
            message += '担当者は実行ログの確認を行ってください。\n\n'
            message += f'対象サイト：{select.text}\n'
            message += '[/info]\n'
            sendChatworkNotification(message)
            exit(1)
        select.click()

        logger.info('importCsvFromAfb: select site')
        driver.implicitly_wait(30)

        driver.find_element_by_xpath(f'//input[@value="{da}"]').click()
        logger.info('importCsvFromAfb: select date range')
        driver.implicitly_wait(30)

        driver.find_element_by_xpath('//input[@src="/assets/img/report/btn_original_csv.gif"]').click()
        sleep(10)

        driver.close()
        driver.quit()
    except Exception as err:
        logger.debug(f'Error: importCsvFromAfb: {err}')
        exit(1)

def importCsvFromLinkA(downloadsDirPath, d):
    url = "https://link-ag.net/partner/sign_in"
    login = os.environ['LINKA_ID']
    password = os.environ['LINKA_PASS']

    ua = UserAgent()
    logger.debug(f'importCsvFromLinkA: UserAgent: {ua.chrome}')

    options = Options()
    options.add_argument(f'user-agent={ua.chrome}')

    prefs = {
        "profile.default_content_settings.popups": 1,
        "download.default_directory":
                os.path.abspath(downloadsDirPath),
        "directory_upgrade": True
    }
    options.add_experimental_option("prefs", prefs)

    try:
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        driver.get(url)
        driver.maximize_window()
        driver.implicitly_wait(30)

        driver.find_element_by_id('login_id').send_keys(login)
        driver.find_element_by_id('password').send_keys(password)
        driver.find_element_by_xpath('//input[@type="submit"]').click()

        logger.debug('importCsvFromLinkA: linka login')
        driver.implicitly_wait(60)

        driver.find_element_by_xpath('//a[@href="/partner/achievements"]').click()
        driver.implicitly_wait(30)

        driver.find_elements_by_id('occurrence_time_occurrence_time')[d].click()
        driver.implicitly_wait(30)

        logger.info('importCsvFromLinkA: select date range')
        driver.implicitly_wait(30)

        driver.find_element_by_xpath('//input[@value="検索"]').click()
        driver.implicitly_wait(30)

        dropdown = driver.find_element_by_id("separator")
        select = Select(dropdown)
        select.select_by_value('comma')
        driver.implicitly_wait(30)

        driver.find_element_by_class_name('partnerMain-btn-md').click()
        sleep(10)

        driver.close()
        driver.quit()
    except Exception as err:
        logger.debug(f'Error: importCsvFromLinkA: {err}')
        exit(1)

def getLatestDownloadedFileName(downloadsDirPath):
    if len(os.listdir(downloadsDirPath)) == 0:
        return None
    return max (
        [downloadsDirPath + '/' + f for f in os.listdir(downloadsDirPath)],
        key=os.path.getctime
    )

def sendChatworkNotification(message):
    try:
        url = f'https://api.chatwork.com/v2/rooms/{os.environ["CHATWORK_ROOM_ID"]}/messages'
        headers = { 'X-ChatWorkToken': os.environ["CHATWORK_API_TOKEN"] }
        params = { 'body': message }
        requests.post(url, headers=headers, params=params)
    except Exception as err:
        logger.error(f'Error: sendChatworkNotification: {err}')
        exit(1)

def get_unique_list(seq):
    seen = []
    return [x for x in seq if x not in seen and not seen.append(x)]

### Google ###
def getGoogleCsvData(csvPath):
    with open(csvPath, newline='', encoding='cp932') as csvfile:
        buf = csv.reader(csvfile, delimiter=',', lineterminator='\r\n', skipinitialspace=True)
        next(buf)
        for row in buf:
            index = row[16].find('gclid=')
            if index == -1:
                continue
            gclid = row[16].split('gclid=')[1]
            date = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            tdate = date.strftime('%Y/%m/%d %H:%M:%S')
            yield [gclid, 'real_cv2', tdate, row[9], 'JPY']

def getGoogleCsvDataLinkA(csvPath):
    with open(csvPath, newline='', encoding='utf-16-le') as csvfile:
        buf = csv.reader(csvfile, delimiter=',', lineterminator='\r\n', skipinitialspace=True)
        next(buf)
        for row in buf:
            print(row)
            index = row[13].find('gclid=')
            if index == -1:
                continue
            gclid = row[13].split('gclid=')[1]
            reward = int(row[6]) / 1.1
            yield [gclid, 'real_cv2', row[2], round(reward), 'JPY']

def writeUploadData(data):
    try:
        SPREADSHEET_ID = os.environ['CONVERSION_IMPORT_SSID']
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('spreadsheet.json', scope)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(SPREADSHEET_ID).worksheet('conversion-import-template')

        sheet.clear()
        sheet.update_acell('A1', 'Parameters:TimeZone= Asia/Tokyo;')
        sheet.update_acell('A2', 'Google Click ID')
        sheet.update_acell('B2', 'Conversion Name')
        sheet.update_acell('C2', 'Conversion Time')
        sheet.update_acell('D2', 'Conversion Value')
        sheet.update_acell('E2', 'Conversion Currency')

        length = len(data)
        if length == 0:
            message = "[info][title]【Google】オフラインコンバージョンのインポート結果[/title]\n"
            message += f'昨日のGSN発生件数は {length} 件です。[/info]'
            sendChatworkNotification(message)
            return

        cell_list = sheet.range(f'A3:E{2 + length}')
        i = 0
        for cell in cell_list:
            if i % 5 == 0:
                cell.value = data[int(i / 5)][0]
            if i % 5 == 1:
                cell.value = data[int(i / 5)][1]
            if i % 5 == 2:
                cell.value = data[int(i / 5)][2]
            if i % 5 == 3:
                cell.value = data[int(i / 5)][3]
            if i % 5 == 4:
                cell.value = data[int(i / 5)][4]
            i += 1

        sheet.update_cells(cell_list, value_input_option='USER_ENTERED')
        message = "[info][title]【Google】オフラインコンバージョンのインポート結果[/title]\n"
        message += 'スプレッドシートへのデータ入力が完了しました。\n'
        message += '入力されたデータは 7:00AM に自動でアップロードされます。\n\n'
        message += f'昨日のGSN発生件数は {length} 件です。[/info]'
        sendChatworkNotification(message)
        return
    except Exception as err:
        logger.debug(f'Error: writeUploadData: {err}')
        exit(1)

### Yahoo! ###
def getYahooCsvData(csvPath):
    with open(csvPath, newline='', encoding='cp932') as csvfile:
        buf = csv.reader(csvfile, delimiter=',', lineterminator='\r\n', skipinitialspace=True)
        next(buf)
        for row in buf:
            index = row[16].find('yclid=YSS')
            if index == -1:
                continue
            yclid = row[16].split('yclid=')[1]
            date = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            tdate = date.strftime('%Y%m%d %H%M%S Asia/Tokyo')
            yield [yclid, 'real_cv', tdate, row[9], 'JPY']

def getYahooCsvDataLinkA(csvPath):
    with open(csvPath, newline='', encoding='utf-16-le') as csvfile:
        buf = csv.reader(csvfile, delimiter=',', lineterminator='\r\n', skipinitialspace=True)
        next(buf)
        for row in buf:
            index = row[13].find('yclid=YSS')
            if index == -1:
                continue
            yclid = row[13].split('yclid=')[1]
            reward = int(row[6]) / 1.1
            date = datetime.datetime.strptime(row[2], '%Y/%m/%d %H:%M:%S')
            tdate = date.strftime('%Y%m%d %H%M%S Asia/Tokyo')
            yield [yclid, 'real_cv', tdate, round(reward), 'JPY']

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

def uploadCsvFile(length, outputFileName, outputFilePath):
    try:
        url_api = 'https://ads-search.yahooapis.jp/api/v7/OfflineConversionService/upload'
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
            message += f'昨日のYSS発生件数は {length} 件です。[/info]'
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
            message += f'昨日のYSS発生件数は {length} 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)
        
        uploadId = body['rval']['values'][0]['offlineConversion']['uploadId']
        return [ uploadId ]
    except Exception as err:
        logger.debug(f'Error: uploadCsvFile: {err}')
        exit(1)

def checkUploadStatus(length, uploadId):
    try:
        url_api = f'https://ads-search.yahooapis.jp/api/v7/OfflineConversionService/get'
        headers = { 'Authorization': f'Bearer {getAccessToken()}' }
        params = {
                'accountId': os.environ["YAHOO_ACCOUNT_ID"],
                'uploadIds': uploadId
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
            message += f'昨日のYSS発生件数は {length} 件です。[/info]'
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
            message += f'昨日のYSS発生件数は {length} 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)

        message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
        message += 'インポートが完了しました。\n\n'

        for value in body['rval']['values']:
            result = value['offlineConversion']
            message += f'アップロードID：{result["uploadId"]}\n'
            message += f'アップロード日時：{result["uploadedDate"]}\n'
            message += f'ステータス：{result["processStatus"]}\n\n'

        message += f'昨日のYSS発生件数は {length} 件です。[/info]'
        sendChatworkNotification(message)

    except Exception as err:
        logger.debug(f'Error: checkUploadStatus: {err}')
        exit(1)

def getCsvPath(dirPath, taskName, no, d):
    os.makedirs(dirPath, exist_ok=True)
    logger.debug(f"import_offline_conversion: start import_csv_from_{taskName}")

    if taskName == "linka":
        importCsvFromLinkA(dirPath, d)
    else:
        importCsvFromAfb(dirPath, no, d)

    csvPath = getLatestDownloadedFileName(dirPath)
    logger.info(f"import_offline_conversion: complete download: {csvPath}")

    return csvPath

### main_script ###
if __name__ == '__main__':

    d = 0
    if len(sys.argv) > 1:
        d = int(sys.argv[1])

    try:
        outputDirPath = './output'
        outputFileName = '育毛剤YSS_CV戻し.csv'
        os.makedirs(outputDirPath, exist_ok=True)
        outputFilePath = f'{outputDirPath}/{outputFileName}'

        afbCsvPath1 = getCsvPath('./csv/afb1', 'afb1', '1', d)
        afbCsvPath2 = getCsvPath('./csv/afb2', 'afb2', '2', d)
        linkaCsvPath = getCsvPath('./csv/linka', 'linka', None, d)

        data = list(getGoogleCsvData(afbCsvPath1))
        data.extend(list(getGoogleCsvData(afbCsvPath2)))
        data.extend(list(getGoogleCsvDataLinkA(linkaCsvPath)))
        data = get_unique_list(data)
        logger.info(f'google: {data}')
        writeUploadData(data)

        data = list(getYahooCsvData(afbCsvPath1))
        data.extend(list(getYahooCsvData(afbCsvPath2)))
        data.extend(list(getYahooCsvDataLinkA(linkaCsvPath)))
        data = get_unique_list(data)
        logger.info(f'yahoo: {data}')
        length = len(data)
        if length == 0:
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += '昨日のYSS発生件数は 0 件です。[/info]'
            sendChatworkNotification(message)
            exit(0)
        elif length % 2 != 0:
            logger.info("import_offline_conversion: createCsvFile")
            createCsvFile(data, outputFilePath)

            logger.info("import_offline_conversion: uploadCsvFile")
            uploadId = uploadCsvFile(length, outputFileName, outputFilePath)
        else:
            data2 = [data.pop(0)]

            logger.info("import_offline_conversion: data: createCsvFile")
            createCsvFile(data, outputFilePath)
            logger.info("import_offline_conversion: data: uploadCsvFile")
            uploadId = uploadCsvFile(length, outputFileName, outputFilePath)
            sleep(5)

            logger.info("import_offline_conversion: data2: createCsvFile")
            createCsvFile(data2, outputFilePath)
            logger.info("import_offline_conversion: data2: uploadCsvFile")
            uploadId.extend(uploadCsvFile(length, outputFileName, outputFilePath))

        sleep(15)
        logger.info(f"import_offline_conversion: uploadId -> {uploadId}")
        logger.info("import_offline_conversion: checkUploadStatus")
        checkUploadStatus(length, uploadId)

        logger.info("import_offline_conversion: Finish")
        exit(0)
    except Exception as err:
        logger.debug(f'import_offline_conversion: {err}')
        exit(1)
