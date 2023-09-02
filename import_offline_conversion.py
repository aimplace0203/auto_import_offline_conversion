import csv
import chromedriver_binary
import datetime
import json
import os
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
import sys
from time import sleep

# Load environment variables
from dotenv import load_dotenv
load_dotenv(override=True)

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


class BasicInfo():
    def __init__(self, days=0):
        self.date = today - datetime.timedelta(days=days)
        self.csv_path = './csv'
        os.makedirs(self.csv_path, exist_ok=True)
        self.output_path = './output'
        os.makedirs(self.output_path, exist_ok=True)
        self.output_file_name = '育毛剤YSS_CV戻し.csv'
        self.access_token = self.get_access_token()

        options = Options()
        #options.add_argument('--headless')
        #options.add_argument('--no-sandbox')

        prefs = {
            "profile.default_content_settings.popups": 1,
            "download.default_directory": os.path.abspath(self.csv_path),
            "directory_upgrade": True
        }
        options.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(options=options)
        self.columns = ["YCLID", "コンバージョン名", "コンバージョン発生日時", "1コンバージョンあたりの価値", "通貨コード"]
        self.data = []

    def get_access_token(self):
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
            logger.error(f'Error: get_access_token: {err}')
            exit(1)

    def get_latest_downloaded_csv(self, path):
        if len(os.listdir(path)) == 0:
            return None
        return max (
            [path + '/' + f for f in os.listdir(path)],
            key=os.path.getctime
        )

    def send_chatwork_notification(self, message):
        try:
            url = f'https://api.chatwork.com/v2/rooms/{os.environ["CHATWORK_ROOM_ID"]}/messages'
            headers = { 'X-ChatWorkToken': os.environ["CHATWORK_API_TOKEN"] }
            params = { 'body': message }
            requests.post(url, headers=headers, params=params)
        except Exception as err:
            logger.error(f'Error: sendChatworkNotification: {err}')
            exit(1)

    def import_rentracks(self):
        url = "https://manage.rentracks.jp/manage/login"
        login = os.environ['RENTRACKS_ID']
        password = os.environ['RENTRACKS_PW']

        try:
            self.driver.get(url)
            self.driver.maximize_window()
            self.driver.implicitly_wait(30)

            self.driver.find_element(By.NAME, 'idMailaddress').send_keys(login)
            self.driver.find_element(By.NAME, 'idLoginPassword').send_keys(password)
            self.driver.find_element(By.NAME, 'idButton').click()
            logger.debug('import_rentracks: login')

            self.driver.implicitly_wait(60)
            self.driver.find_element(By.ID, 'main')

            self.driver.get('https://manage.rentracks.jp/manage/detail_sales')
            logger.debug('import_rentracks: move to detail_sales')
            self.driver.implicitly_wait(20)

            select = Select(self.driver.find_element(By.ID, 'idGogoYear'))
            select.select_by_value(self.date.strftime('%Y'))
            select = Select(self.driver.find_element(By.ID, 'idGogoMonth'))
            select.select_by_value(str(int(self.date.strftime('%m'))))
            select = Select(self.driver.find_element(By.ID, 'idGogoDay'))
            select.select_by_value(str(int(self.date.strftime('%d'))))

            select = Select(self.driver.find_element(By.ID, 'idDoneYear'))
            select.select_by_value(self.date.strftime('%Y'))
            select = Select(self.driver.find_element(By.ID, 'idDoneMonth'))
            select.select_by_value(str(int(self.date.strftime('%m'))))
            select = Select(self.driver.find_element(By.ID, 'idDoneDay'))
            select.select_by_value(str(int(self.date.strftime('%d'))))

            sleep(2)
            logger.debug('import_rentracks: select date')

            self.driver.find_element(By.NAME, 'idButtonFD').click()
            sleep(2)

            file_path = self.get_latest_downloaded_csv(self.csv_path)
            logger.debug(f'import_rentracks: file_path -> {file_path}')

            df = pd.read_csv(file_path, encoding='cp932')
            filtered_df = df[
                df['リファラー'].str.contains('yclid=', na=False) & 
                df['サイト名'].str.contains('クリニックフォア', na=False)
            ]

            for index, row in filtered_df.iterrows():
                yclid = row['リファラー'].split('yclid=')[1]
                date_str = row['売上日時']
                clean_date_str = date_str.split('（')[0] + date_str.split('）')[1]
                date = datetime.datetime.strptime(clean_date_str, '%Y/%m/%d %H:%M:%S')
                tdate = date.strftime('%Y%m%d %H%M%S Asia/Tokyo')
                self.data.append([yclid, 'オフラインCV', tdate, '39000', 'JPY'])

            os.remove(file_path)
            logger.debug(f'import_rentracks: data -> {self.data}')

        except Exception as err:
            logger.debug(f'Error: import_rentracks: {err}')
            self.driver.close()
            self.driver.quit()
            exit(1)

    def import_felmat(self):
        url = "https://www.felmat.net/publisher/login"
        login = os.environ['FELMAT_ID']
        password = os.environ['FELMAT_PW']

        try:
            self.driver.get(url)
            self.driver.maximize_window()
            self.driver.implicitly_wait(30)

            self.driver.find_element(By.ID, 'p_username').send_keys(login)
            self.driver.find_element(By.ID, 'p_password').send_keys(password)
            self.driver.find_element(By.NAME, 'partnerlogin').click()
            logger.debug('import_felmat: login')

            self.driver.implicitly_wait(60)
            self.driver.find_element(By.ID, 'top-main-navigation')

            self.driver.get('https://www.felmat.net/publisher/conversion')
            logger.debug('import_rentracks: move to conversion')
            self.driver.implicitly_wait(20)

            self.driver.find_element(By.NAME, 'start_date').clear()
            self.driver.find_element(By.NAME, 'start_date').send_keys(self.date.strftime('%Y-%m-%d'))
            self.driver.find_element(By.NAME, 'end_date').clear()
            self.driver.find_element(By.NAME, 'end_date').send_keys(self.date.strftime('%Y-%m-%d'))
            self.driver.find_element(By.XPATH, "//label[@for='start_date']").click()
            sleep(2)

            self.driver.find_element(By.XPATH, "//button[@name='search']").click()
            sleep(2)

            if not '全サイトが対象' in self.driver.page_source:
                logger.debug('import_felmat: no data')
                return

            self.driver.execute_script("window.scrollTo(0, 0);")
            self.driver.find_element(By.XPATH, "//button[@name='csv_dl']").click()
            sleep(3)

            file_path = self.get_latest_downloaded_csv(self.csv_path)
            logger.debug(f'import_felmat: file_path -> {file_path}')

            df = pd.read_csv(file_path, encoding='cp932')
            filtered_df = df[
                (df['サイトID'] == 104176) & 
                (df['掲載URL'].str.contains('yclid=', na=False))
            ]

            for index, row in filtered_df.iterrows():
                yclid = row['掲載URL'].split('yclid=')[1]
                date = datetime.datetime.strptime(row['発生日時'], '%Y-%m-%d %H:%M:%S')
                tdate = date.strftime('%Y%m%d %H%M%S Asia/Tokyo')
                price = str(int(row['成果報酬（税抜）']))
                self.data.append([yclid, 'オフラインCV', tdate, price, 'JPY'])

            os.remove(file_path)
            logger.debug(f'import_felmat: data -> {self.data}')

        except Exception as err:
            logger.debug(f'Error: import_felmat: {err}')
            self.driver.close()
            self.driver.quit()
            exit(1)


    def create_output_file(self):
        self.driver.close()
        self.driver.quit()
        os.makedirs(self.output_path, exist_ok=True)
        df = pd.DataFrame(self.data, columns=self.columns)
        df.to_csv(f'{self.output_path}/{self.output_file_name}', quoting=csv.QUOTE_ALL, index=False, encoding='shift-jis')


    def upload_offline_cv(self):
        try:
            url_api = 'https://ads-search.yahooapis.jp/api/v11/OfflineConversionService/upload'
            headers = { 'Authorization': f'Bearer {self.access_token}' }
            files = { 'file': open(f'{self.output_path}/{self.output_file_name}', mode='rb') }
            params = {
                'accountId': os.environ["YAHOO_ACCOUNT_ID"],
                'uploadType': 'NEW',
                'uploadFileName': f'{self.output_file_name}'
            }
            req = requests.post(url_api, headers=headers, files=files, params=params)
            body = json.loads(req.text)
            logger.info(f'upload_offline_cv - status_code: {req.status_code}')
            logger.info(f'upload_offline_cv - response:\n--> {req.text}')

            if req.status_code != 200:
                message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
                message += 'インポートに失敗しました。\n'
                message += '担当者は実行ログの確認を行ってください。\n\n'
                message += f'ステータスコード：{req.status_code}\n\n'
                message += f'YSS発生件数は {len(self.data)} 件です。[/info]'
                self.send_chatwork_notification(message)
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
                message += f'YSS発生件数は {len(self.data)} 件です。[/info]'
                self.send_chatwork_notification(message)
                exit(0)

            self.upload_id = body['rval']['values'][0]['offlineConversion']['uploadId']
        except Exception as err:
            logger.debug(f'Error: upload_offline_cv: {err}')
            exit(1)

    def check_upload_status(self):
        try:
            url_api = f'https://ads-search.yahooapis.jp/api/v11/OfflineConversionService/get'
            headers = { 'Authorization': f'Bearer {self.access_token}' }
            params = {
                'accountId': os.environ["YAHOO_ACCOUNT_ID"],
                'uploadIds': [self.upload_id]
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
                message += f'YSS発生件数は {len(self.data)} 件です。[/info]'
                self.send_chatwork_notification(message)
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
                message += f'YSS発生件数は {len(self.data)} 件です。[/info]'
                self.send_chatwork_notification(message)
                exit(0)

            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += 'インポートが完了しました。\n\n'

            for value in body['rval']['values']:
                result = value['offlineConversion']
                message += f'アップロードID：{result["uploadId"]}\n'
                message += f'アップロード日時：{result["uploadedDate"]}\n'
                message += f'ステータス：{result["processStatus"]}\n\n'

            message += f'YSS発生件数は {len(self.data)} 件です。[/info]'
            self.send_chatwork_notification(message)

        except Exception as err:
            logger.debug(f'Error: check_upload_status: {err}')
            exit(1)


### main_script ###
if __name__ == '__main__':

    days = 0
    if len(sys.argv) > 1:
        days = int(sys.argv[1])

    try:
        bi = BasicInfo(days)
        bi.import_rentracks()
        bi.import_felmat()
        bi.create_output_file()

        if len(bi.data) > 0:
            bi.upload_offline_cv()
            sleep(10)
            bi.check_upload_status()
        else:
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果[/title]\n"
            message += 'インポート対象のデータがありませんでした。[/info]'
            bi.send_chatwork_notification(message)

        exit(0)
    except Exception as err:
        logger.debug(f'import_offline_conversion: {err}')
        exit(1)
