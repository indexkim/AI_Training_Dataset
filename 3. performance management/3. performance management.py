#!/usr/bin/env python
# coding: utf-8



import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import chromedriver_autoinstaller
import subprocess
import shutil
import pyautogui
import os
import glob
import re
import pandas as pd
import openpyxl
from openpyxl import load_workbook
import xlsxwriter
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine
import requests


def slack_id_csv_download():
    try:
        shutil.rmtree(r"C:\chrometemp")  
    except FileNotFoundError:
        pass

    try:
        subprocess.Popen(r'C:\Program Files\Google\Chrome\Application\chrome.exe --remote-debugging-port=9222 '
                         r'--user-data-dir="C:\chrometemp"')  

    except FileNotFoundError:
        subprocess.Popen(r'C:\Users\Jisoo\AppData\Local\Google\Chrome\Application\chrome.exe --remote-debugging-port=9222 '
                         r'--user-data-dir="C:\chrometemp"')

    option = Options()
    option.add_experimental_option('debuggerAddress', '127.0.0.1:9222')

    chrome_ver = chromedriver_autoinstaller.get_chrome_version().split('.')[0]
    try:
        driver = webdriver.Chrome(
            f'./{chrome_ver}/chromedriver.exe', options=option)

    except:
        chromedriver_autoinstaller.install(True)
        driver = webdriver.Chrome(
            f'./{chrome_ver}/chromedriver.exe', options=option)
    driver.implicitly_wait(10)

    driver.get('https://Labeling_Home.slack.com/admin/stats#members')
    driver.find_element_by_xpath('//*[@id="google_login_button"]').click()

    pyautogui.write('js48961269@gmail.com') # Google 계정으로 로그인
    pyautogui.press('enter')
    time.sleep(3)
    pyautogui.write('password') 
    pyautogui.press('enter')
    time.sleep(5)
    driver.implicitly_wait(5)

    driver.get('https://Labeling_Home.slack.com/admin/stats#members') # Slack Workspace

    driver.find_element_by_xpath(
        '//*[@id="page_contents"]/section/div[2]/div[5]/button').click()


def slack_id_csv_rename(year, month, date, yyyymmdd):
    csv_path = 'C:/Users/Jisoo/Downloads'
    csv_file = csv_path+'/Labeling_Home 멤버 분석 이전 30일 - '+year+'년 '+month+'월 '+date+'일.csv' 
    os.rename(csv_file, csv_path+'/slack_id_'+yyyymmdd+'.csv')


def slack_id_find(yyyymmdd):
    csv_path = 'C:/Users/Jisoo/Downloads'
    slack_id_csv = csv_path+'/slack_id_'+str(yyyymmdd)+'.csv' # Slack id
    df1 = pd.read_csv(slack_id_csv, encoding='utf-8')
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx' # 일일 분배 일정
    df2 = pd.read_excel(labeling_workload_xlsx, sheet_name='Sheet1') 
    df3 = df2.join(df1.set_index('이름')['사용자 ID'], on='작업자')  # Slack id를 작업자 코드와 매치
    member_id = list(df3['작업자코드']+':<@'+df3['사용자 ID']+'|cal>') # Tag
    return member_id


def rework_message(yyyymmdd):
    member_id = slack_id_find(yyyymmdd)

    def post_message(token, channel, text):
        response = requests.post('https://slack.com/api/chat.postMessage',
                                 headers={'Authorization': 'Bearer' + token},
                                 data={'channel': channel, 'text': text}
                                 )
        print(response)

    # Slack Api  - Bot User OAuth Token for 
    myToken = 'my-slack-token-000000000000000000000000'

    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx'
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]

    labeling_before = []
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_before/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                labeling_before.append(labeling_path)

                
    labeling_after = []
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_after/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                labeling_after.append(labeling_path)

                
    # 재작업 폴더 알림
    uploaded_list = set()
    rework_list = set()
    for labeling_a in labeling_after:
        uploaded_cnt = 0
        rework_cnt = 0
        for folder in sorted(os.listdir(labeling_a)):
            uploaded_cnt += 1
            uploaded_list.add(folder[:17])
            jpg_cnt = 0
            json_cnt = 0
            for file in sorted(os.listdir(labeling_a+'/'+folder)):
                if file.endswith('jpg'):
                    jpg_cnt += 1
                elif file.endswith('Json'):
                    json_cnt += 1
            if json_cnt != jpg_cnt:
                rework_cnt += 1
                rework_list.add(folder)
            if folder in rework_list:
                for member in member_id:
                    if member[:4] == labeling_a[-19:-15]:  # 해당 작업자 태그 및 내용 전송
                        post_message(
                            myToken, '#rework_notice', member[5:]+'님,'+yyyymmdd+'일 작업물 중 재작업 대상 폴더가 있습니다. '+str(rework_cnt)+'.'+folder)

                        
    # 미업로드 폴더 알림
    for labeling_b in labeling_before:
        distributed_cnt = 0
        missed_cnt = 0
        for folder in sorted(os.listdir(labeling_b)):
            distributed_cnt += 1
            if folder in uploaded_list:
                pass
            else:
                missed_cnt += 1
                m_cnt = len(os.listdir(labeling_b)) - uploaded_cnt
                for member in member_id:
                    if member[:4] == labeling_b[-20:-16]:  # 해당 작업자 태그 및 내용 전송
                        post_message(myToken, '#rework_notice', member[5:]+'님,'+yyyymmdd+'일 작업량 '+str(len(
                            os.listdir(labeling_b)))+'개 중 '+str(m_cnt)+'개가 업로드되지 않았습니다. '+str(missed_cnt)+'. '+folder)

                        
    # 관리자용 채널에 작업자별 작업 내역 전송
    post_message(myToken, '#labeling_admin', labeling_a[-19:]+'/분배개수:'+str(
        distributed_cnt)+'/업로드개수:'+str(uploaded_cnt)+'/미업로드개수:'+str(m_cnt)+'/재작업개수:'+str(rework_cnt))


schedule.every().days.at('10:00').do(rework_message)


while True:
    schedule.run_pending()
    time.sleep(1)


def labeling_after(yyyymmdd):
    result = pd.DataFrame(columns=['PATH', 'FOLDER', 'STATUS'])
    storage_path = 'X:/localUser/TrainingData/Refinement/refine_pass/'+yyyymmdd+'_정제완료'
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx'
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]
    workload_count = [df.iloc[row, 1] for row in range(len(df))]
    workload_dict = dict(zip(member_list, workload_count))
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_after/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                for folder in sorted(os.listdir(labeling_path)):
                    if len(folder) == 17:
                        if re.findall(r'^[0-2][0-9]_X[0-9][0-9][0-9]_C[0-9][0-9][0-9]_[0-1][0-9][0-3][0-9]$', folder):
                            path_r = labeling_path[-20:]
                            result = result.append(pd.DataFrame([[path_r, folder, 'pass']], columns=[
                                'PATH', 'FOLDER', 'STATUS']), ignore_index=True)
                        else: #len(folder) == 17이나 regex에 맞지 않는 경우
                            path_r = labeling_path[-20:]
                            result = result.append(pd.DataFrame([[path_r, folder, 'name']], columns=[
                                'PATH', 'FOLDER', 'STATUS']), ignore_index=True)                            
                    else:  #len(folder) > 17의 경우 데이터셋 이름 17자 이후 에러 코드가 붙은 형태
                        path_r = labeling_path[-20:]
                        result = result.append(pd.DataFrame([[path_r, folder, folder[17:]]], columns=[
                            'PATH', 'FOLDER', 'STATUS']), ignore_index=True)

    return result


def labeling_after_xlsx(yyyymmdd):
    df = labeling_after(yyyymmdd)
    labeling_after_xlsx = 'X:/localUser/TrainingData/data/labeling_data/labeling_after/' +         'labeling_after_'+str(yyyymmdd)+'.xlsx'
    with pd.ExcelWriter(labeling_after_xlsx, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=str(yyyymmdd))
        writer.book.use_zip64()


def labeling_after_sql(yyyymmdd):
    df = labeling_after(yyyymmdd)
    pymysql.install_as_MySQLdb()
    engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(
        user='root', pw='password', db='labeling_after'))
    conn = engine.connect()
    labeling_after_sql = 'labeling_after_'+str(yyyymmdd)
    df.to_sql(name=labeling_after_sql,
              con=engine, if_exists='fail')
    conn.close()


def labeling_after_class(yyyymmdd):
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx'
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]
    count_dict = {'01': 0, '02': 0, '03': 0, '04': 0, '05': 0, '06': 0, '07': 0, '08': 0, '09': 0, '10': 0, '11': 0, '12': 0, '13': 0, '14': 0, '15': 0, '16': 0, '17': 0, '18': 0,
                  '19': 0, '20': 0, '21': 0, '22': 0, '23': 0, '24': 0, '25': 0}
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_after/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                for folder in sorted(os.listdir(labeling_path)):
                    count_dict[folder[0:2]] += 1

    df = pd.DataFrame.from_dict(
        count_dict, orient='index', columns=[str(yyyymmdd)+'_분배'])
    return df, df.sum()


def labeling_finish_notice(yyyymmdd):
    def post_message(token, channel, text):
        response = requests.post('https://slack.com/api/chat.postMessage',
                                 headers={'Authorization': 'Bearer' + token},
                                 data={'channel': channel, 'text': text}
                                 )
        print(response)

    myToken = 'my-slack-token-000000000000000000000000'

    post_message(myToken, '#notice_me', str(yyyymmdd) +
                 '_performance management_완료')

