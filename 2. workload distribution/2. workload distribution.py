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


def labeling_mkdir(yyyymmdd):
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/' +         str(yyyymmdd)+'_분배.xlsx'  # 일별 작업자 목록 & 작업량
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob(r'X:/localUser/TrainingData/Labeling/**/**', recursive=False)):
            if member in labeling_path:
                try:
                    os.mkdir(labeling_path + '/label_before/' +
                             member + '_' + yyyymmdd + '_before')
                    os.mkdir(labeling_path + '/label_after/' +
                             member + '_' + yyyymmdd + '_after')
                except FileExistsError:
                    pass


def labeling_distribution(yyyymmdd):
    result = pd.DataFrame(columns=['PATH', 'FOLDER'])
    storage_path = 'X:/localUser/TrainingData/Refinement/refine_pass/'+yyyymmdd+'_정제완료'
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx'
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]
    workload_count = [df.iloc[row, 1] for row in range(len(df))]
    workload_dict = dict(zip(member_list, workload_count))
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_before/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                while len(os.listdir(labeling_path)) < workload_dict[member]:
                    for folder in sorted(os.listdir(storage_path)):
                        if len(os.listdir(labeling_path)) < workload_dict[member]:
                            shutil.move(storage_path + '/' + folder,
                                        labeling_path + '/' + folder)
                        else:
                            break
                    break


# 작업자 추가 분배 요청시 수동 기입
def labeling_distribution_more(yyyymmdd, distribute_count, member):
    storage_path = 'X:/localUser/TrainingData/Refinement/refine_pass/'+yyyymmdd+'_정제완료'
    for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_before/**', recursive=False)):
        if member in labeling_path and yyyymmdd in labeling_path:
            while len(os.listdir(labeling_path)) < distribute_count:
                for folder in sorted(os.listdir(storage_path)):
                    if len(os.listdir(labeling_path)) < distribute_count:
                        shutil.move(storage_path + '/' + folder,
                                    labeling_path + '/' + folder)
                    else:
                        break
                break


def labeling_before(yyyymmdd):
    result = pd.DataFrame(columns=['PATH', 'FOLDER'])
    storage_path = 'X:/localUser/TrainingData/Refinement/refine_pass/'+yyyymmdd+'_정제완료'
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx'
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]
    workload_count = [df.iloc[row, 1] for row in range(len(df))]
    workload_dict = dict(zip(member_list, workload_count))
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_before/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                for folder in sorted(os.listdir(labeling_path)):
                    path_r = labeling_path[-20:]
                    result = result.append(pd.DataFrame([[path_r, folder]], columns=[
                        'PATH', 'FOLDER']), ignore_index=True)
    return result


def labeling_before_xlsx(yyyymmdd):
    df = labeling_before(yyyymmdd)
    labeling_before_xlsx = 'X:/localUser/TrainingData/data/labeling_data/labeling_before/' +         'labeling_before_'+str(yyyymmdd)+'.xlsx'
    with pd.ExcelWriter(labeling_before_xlsx, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=str(yyyymmdd))
        writer.book.use_zip64()


def labeling_before_sql(yyyymmdd):
    df = labeling_before(yyyymmdd)
    pymysql.install_as_MySQLdb()
    engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(
        user='root', pw='password', db='labeling_before'))
    conn = engine.connect()
    labeling_before_sql = 'labeling_before_'+str(yyyymmdd)
    df.to_sql(name=labeling_before_sql,
              con=engine, if_exists='fail')
    conn.close()


def labeling_before_class(yyyymmdd):
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx'
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]
    count_dict = {'01': 0, '02': 0, '03': 0, '04': 0, '05': 0, '06': 0, '07': 0, '08': 0, '09': 0, '10': 0, '11': 0, '12': 0, '13': 0, '14': 0, '15': 0, '16': 0, '17': 0, '18': 0,
                  '19': 0, '20': 0, '21': 0, '22': 0, '23': 0, '24': 0, '25': 0}
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_before/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                for folder in sorted(os.listdir(labeling_path)):
                    count_dict[folder[0:2]] += 1

    df = pd.DataFrame.from_dict(
        count_dict, orient='index', columns=[str(yyyymmdd)+'_분배'])
    return df, df.sum()


def distribution_finish_notice(yyyymmdd):
    def post_message(token, channel, text):
        response = requests.post('https://slack.com/api/chat.postMessage',
                                 headers={'Authorization': 'Bearer' + token},
                                 data={'channel': channel, 'text': text}
                                 )
        print(response)

    myToken = 'my-slack-token-000000000000000000000000'

    post_message(myToken, '#notice_me', str(yyyymmdd) +
                 '_workload distribution_완료')

