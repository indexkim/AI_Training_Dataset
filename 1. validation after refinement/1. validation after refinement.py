#!/usr/bin/env python
# coding: utf-8


import os
import glob
import re
import shutil
import pandas as pd
import openpyxl
from openpyxl import load_workbook
import xlsxwriter
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine
import requests
import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase


yyyymmdd = '20210701' #날짜 입력


def get_date(yyyymmdd):
    refine_path = 'X:/localUser/TrainingData/Refinement/refine_pass'
    path = refine_path+'/'+yyyymmdd+'_정제완료'
    return path


def refinement_raw(yyyymmdd):
    result = pd.DataFrame(columns=['PATH', 'FOLDER'])
    path = get_date(yyyymmdd)
    path_r = yyyymmdd+'_정제완료'
    for folder in sorted(os.listdir(path)):
        result = result.append(pd.DataFrame([[path_r, folder]], columns=[
            'PATH', 'FOLDER']), ignore_index=True)
    return result


def refinement_raw_xlsx(yyyymmdd):
    df = refinement_raw(yyyymmdd)
    refinement_raw_xlsx = 'X:/localUser/TrainingData/Refinement/refine_data/raw/' +         'refinement_raw_'+str(yyyymmdd)+'.xlsx'
    with pd.ExcelWriter(refinement_raw_xlsx, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=str(yyyymmdd))
        writer.book.use_zip64()


def refinement_raw_sql(yyyymmdd):
    df = refinement_raw(yyyymmdd)
    pymysql.install_as_MySQLdb()
    engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(
        user='root', pw='2772', db='refinement_raw'))
    conn = engine.connect()
    refinement_raw_sql = 'refinement_raw_'+str(yyyymmdd)
    refinement_raw_sql_all = 'refinement_raw_all'
    df.to_sql(name=refinement_raw_sql,
              con=engine, if_exists='fail')
    df.to_sql(name=refinement_raw_sql_all, con=engine, if_exists='append')
    conn.close()


def refinement_pass(yyyymmdd):
    result = pd.DataFrame(columns=['PATH', 'FOLDER', 'STATUS'])
    conn = pymysql.connect(host='127.0.0.1', user='root', password='password', db='refinement_pass_all', charset='utf8',
                           autocommit=True, cursorclass=pymysql.cursors.DictCursor)
    sql = 'select FOLDER from refinement_pass_all'
    df = pd.read_sql(sql, conn)
    same_list = [df.iloc[row, 0] for row in range(len(df))]
    path = get_date(yyyymmdd)
    path_r = yyyymmdd+'_정제완료'

    for folder in sorted(os.listdir(path)):
        if re.findall(r'^[0-2][0-9]_X[0-9][0-9][0-9]_C[0-9][0-9][0-9]_[0-1][0-9][0-3][0-9]$', folder):
            jpg_cnt = 0
            for file in sorted(os.listdir(path+'/'+folder)):
                if re.findall(r'^[0-2][0-9]_X[0-9][0-9][0-9]_C[0-9][0-9][0-9]_[0-1][0-9][0-3][0-9]_[0-9].jpg$', file):
                    jpg_cnt += 1
                else:
                    print(path_r, folder, 'name')
                    result = result.append(pd.DataFrame([[path_r, folder, 'name']], columns=[
                        'PATH', 'FOLDER', 'STATUS']), ignore_index=True)
        else:
            print(path_r, folder, 'name')
            result = result.append(pd.DataFrame([[path_r, folder, 'name']], columns=[
                'PATH', 'FOLDER', 'STATUS']), ignore_index=True)

    for folder in sorted(os.listdir(path)):
        if re.findall(r'^[0-2][0-9]_X[0-9][0-9][0-9]_C[0-9][0-9][0-9]_[0-1][0-9][0-3][0-9]$', folder):
            if len(os.listdir(path+'/'+folder)) == jpg_cnt:
                if folder in same_list:
                    print(path_r, folder, 'same')
                    result = result.append(pd.DataFrame([[path_r, folder, 'same']], columns=[
                        'PATH', 'FOLDER', 'STATUS']), ignore_index=True)
                elif folder not in same_list:
                    print(path_r, folder, 'pass')
                    result = result.append(pd.DataFrame([[path_r, folder, 'pass']], columns=[
                        'PATH', 'FOLDER', 'STATUS']), ignore_index=True)

    result = result[result['STATUS'] == 'pass']
    return result


def refinement_pass_xlsx(yyyymmdd):
    df = refinement_pass(yyyymmdd)
    refinement_pass_xlsx = 'X:/localUser/TrainingData/Refinement/refine_data/pass/' +         'refinement_pass_'+str(yyyymmdd)+'.xlsx'
    with pd.ExcelWriter(refinement_pass_xlsx, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=str(yyyymmdd))
        writer.book.use_zip64()


def refinement_pass_sql(yyyymmdd):
    df = refinement_pass(yyyymmdd)
    pymysql.install_as_MySQLdb()
    engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(
        user='root', pw='2772', db='refinement_pass'))
    conn = engine.connect()
    refinement_pass_sql = 'refinement_pass_'+str(yyyymmdd)
    refinement_pass_sql_all = 'refinement_pass_all'
    df.to_sql(name=refinement_pass_sql,
              con=engine, if_exists='fail')
    df.to_sql(name=refinement_pass_sql_all, con=engine, if_exists='append')
    conn.close()


def refinement_error(yyyymmdd):
    result = pd.DataFrame(columns=['PATH', 'FOLDER', 'STATUS'])
    conn = pymysql.connect(host='127.0.0.1', user='root', password='password', db='refinement_pass_all', charset='utf8',
                           autocommit=True, cursorclass=pymysql.cursors.DictCursor)
    sql = 'select FOLDER from refinement_pass_all'
    df = pd.read_sql(sql, conn)
    same_list = [df.iloc[row, 0] for row in range(len(df))]
    path = get_date(yyyymmdd)
    path_r = yyyymmdd+'_정제완료'

    for folder in sorted(os.listdir(path)):
        if re.findall(r'^[0-2][0-9]_X[0-9][0-9][0-9]_C[0-9][0-9][0-9]_[0-1][0-9][0-3][0-9]$', folder):
            jpg_cnt = 0
            for file in sorted(os.listdir(path+'/'+folder)):
                if re.findall(r'^[0-2][0-9]_X[0-9][0-9][0-9]_C[0-9][0-9][0-9]_[0-1][0-9][0-3][0-9]_[0-9].jpg$', file):
                    jpg_cnt += 1
                else:
                    print(path_r, folder, 'name')
                    result = result.append(pd.DataFrame([[path_r, folder, 'name']], columns=[
                        'PATH', 'FOLDER', 'STATUS']), ignore_index=True)
        else:
            print(path_r, folder, 'name')
            result = result.append(pd.DataFrame([[path_r, folder, 'name']], columns=[
                'PATH', 'FOLDER', 'STATUS']), ignore_index=True)

    for folder in sorted(os.listdir(path)):
        if len(os.listdir(path+'/'+folder)) == jpg_cnt:
            if folder in same_list:
                print(path_r, folder, 'same')
                result = result.append(pd.DataFrame([[path_r, folder, 'same']], columns=[
                    'PATH', 'FOLDER', 'STATUS']), ignore_index=True)

    return result


def refinement_error_xlsx(yyyymmdd):
    df = refinement_error(yyyymmdd)
    refinement_error_xlsx = 'X:/localUser/TrainingData/Refinement/refine_data/error/' +         'refinement_error_'+str(yyyymmdd)+'.xlsx'
    with pd.ExcelWriter(refinement_error_xlsx, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=str(yyyymmdd))
        writer.book.use_zip64()


def refinement_error_sql(yyyymmdd):
    df = refinement_error(yyyymmdd)
    pymysql.install_as_MySQLdb()
    engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(
        user='root', pw='2772', db='refinement_error'))
    conn = engine.connect()
    refinement_error_sql = 'refinement_error_'+str(yyyymmdd)
    refinement_error_sql_all = 'refinement_error_all'
    df.to_sql(name=refinement_error_sql,
              con=engine, if_exists='fail')
    df.to_sql(name=refinement_error_sql_all, con=engine, if_exists='append')
    conn.close()


def refinement_error_move(yyyymmdd):
    df = refinement_error(yyyymmdd)
    path = get_date(yyyymmdd)
    df1 = df[(df['PATH'].str.contains(yyyymmdd)) & (df['STATUS'] == 'name')]
    df2 = df[(df['PATH'].str.contains(yyyymmdd)) & (df['STATUS'] == 'same')]
    name_list = [df1.iloc[row, 2] for row in range(len(df1))]
    same_list = [df2.iloc[row, 2] for row in range(len(df2))]
    name_path = 'X:/localUser/TrainingData/Refinement/refine_name'
    same_path = 'X:/localUser/TrainingData/Refinement/refine_same'
    for folder in sorted(os.listdir(path)):
        if folder in name_list:
            try:
                shutil.move(path+'/'+folder, name_path+'/'+folder)
            except:
                print(path+'/'+folder, name_path+'/'+folder, 'failed to move')
        elif folder in same_list:
            try:
                shutil.move(path+'/'+folder, same_path+'/'+folder)
            except:
                print(path+'/'+folder, same_path+'/'+folder, 'failed to move')


def refinement_error_mail(yyyymmdd):
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.ehlo()
    smtp.starttls()
    smtp.login('mymail@gmail.com', 'mypassword')

    refine_mail = ['refine_mail@gmail.com']  # 정제 담당자 메일 주소
    msg = MIMEMultipart()
    msg['Subject'] = str(yyyymmdd)+'_정제 부적합'  # 제목
    part = MIMEText(str(yyyymmdd)+'일자 정제 부적합 폴더 목록입니다.')  # 내용
    msg.attach(part)

    filepath = refinement_error_xlsx

    with open(filepath, 'rb') as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=filepath)
        msg.attach(part)
    msg

    for address in refine_mail:
        msg['To'] = address
        smtp.sendmail('mymail@gmail.com', address, msg.as_string())
        print(address)


def refinement_finish_notice(yyyymmdd):
    def post_message(token, channel, text):
        response = requests.post('https://slack.com/api/chat.postMessage',
                                 headers={'Authorization': 'Bearer' + token},
                                 data={'channel': channel, 'text': text}
                                 )
        print(response)

    # Slack Api  - Bot User OAuth Token
    myToken = 'my-slack-token-000000000000000000000000'

    post_message(myToken, '#notice_me', str(yyyymmdd) +
                 '_validation after refinement가 완료되었습니다.')

