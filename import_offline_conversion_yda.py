import csv
import chromedriver_binary
import datetime
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials
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
        self.columns = ["YCLID", "コンバージョン名", "コンバージョン発生日時", "1コンバージョンあたりの価値"]
        self.data = []

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
#            select.select_by_value(str(int(self.date.strftime('%m'))))
            select.select_by_value("9")
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
                yclid = yclid.split('&')[0]
                date_str = row['売上日時']
                clean_date_str = date_str.split('（')[0] + date_str.split('）')[1]
                date = datetime.datetime.strptime(clean_date_str, '%Y/%m/%d %H:%M:%S')
                tdate = date.strftime('%Y%m%d %H%M%S Asia/Tokyo')
                if yclid.startswith('YJAD'):
                    self.data.append([yclid, 'オフラインCV', tdate, '39000'])
            
            logger.debug(f'import_rentracks: data -> {self.data}')
            self.driver.close()
            self.driver.quit()

        except Exception as err:
            logger.debug(f'Error: import_rentracks: {err}')
            self.driver.close()
            self.driver.quit()
            exit(1)

    def update_data(self):
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name('spreadsheet.json', scope)
            gc = gspread.authorize(credentials)
            sheet = gc.open_by_key(os.environ['GOOGLE_SHEET_KEY']).worksheet('Data2')

            sheet.clear()
            sheet.append_row(self.columns)
            sheet.append_rows(self.data)

            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果(YDA)[/title]\n"
            message += 'インポートが完了しました。[/info]'
            self.send_chatwork_notification(message)
        except Exception as err:
            logger.debug(f'Error: updateData: {err}')
            exit(1)


### main_script ###
if __name__ == '__main__':

    days = 1
    if len(sys.argv) > 1:
        days = int(sys.argv[1])

    try:
        bi = BasicInfo(days)
        bi.import_rentracks()

        if len(bi.data) > 0:
            bi.update_data()
        else:
            message = "[info][title]【Yahoo!】オフラインコンバージョンのインポート結果(YDA)[/title]\n"
            message += 'インポート対象のデータがありませんでした。[/info]'
            bi.send_chatwork_notification(message)

        exit(0)
    except Exception as err:
        logger.debug(f'import_offline_conversion: {err}')
        exit(1)
