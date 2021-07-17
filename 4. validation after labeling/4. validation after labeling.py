#!/usr/bin/env python
# coding: utf-8



import os
import glob
import pandas as pd
import json
import shutil
import requests
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders


def labeling_validation_all(yyyymmdd):

    class_dict = {  # 01번부터 25번까지 총 25가지의 클래스가 있으며, 클래스별로 상세항목이 존재한다. 여기서는 간략히 표시한다.

        '01': ['a', 'b', 'c', 'd', 'e'],
        '02': ['aa', 'bb', 'cc', 'dd', 'ee'],
        '03': ['aaa', 'bbb', 'ccc', 'ddd', 'eee'],
        ...
        '25': ['aaaaa', 'bbbbb', 'ccccc', 'ddddd', 'eeeee']

    }

    count_dict_box = {  # 상세항목별 수량 집계용 dict
        '01_a': 0,
        '01_b': 0,
        '01_c': 0,
        '01_d': 0,
        '01_e': 0,
        '02_aa': 0,
        '02_bb': 0,
        '02_cc': 0,
        '02_dd': 0,
        '02_ee': 0,
        '03_aaa': 0,
        '03_bbb': 0,
        '03_ccc': 0,
        '03_ddd': 0,
        '03_eee': 0,
        ...
        '25_aaaaa': 0,
        '25_bbbbb': 0,
        '25_ccccc': 0,
        '25_ddddd': 0,
        '25_eeeee': 0
    }

    label_fin_dict = {'26': 0, '27': 0, '28': 0, '29': 0, '30': 0, '31': 0, '32': 0, '33': 0, '34': 0, '01': 0, '02': 0, '03': 0, '04': 0, '05': 0, '06': 0, '07': 0, '08': 0, '09': 0, '10': 0, '44': 0, '45': 0, '46': 0, '47': 0, '48': 0, '49': 0, '50': 0, '11': 0, '12': 0, '13': 0, '14': 0, '15': 0, '16': 0, '17': 0, '18': 0,
                      '19': 0, '20': 0, '21': 0, '22': 0, '23': 0, '24': 0, '25': 0}
    label_pass_dict = {'01': 0, '02': 0, '03': 0, '04': 0, '05': 0, '06': 0, '07': 0, '08': 0, '09': 0, '10': 0, '11': 0, '12': 0, '13': 0, '14': 0, '15': 0, '16': 0, '17': 0, '18': 0,
                       '19': 0, '20': 0, '21': 0, '22': 0, '23': 0, '24': 0, '25': 0}
    label_jpg_dict = {'01': 0, '02': 0, '03': 0, '04': 0, '05': 0, '06': 0, '07': 0, '08': 0, '09': 0, '10': 0, '11': 0, '12': 0, '13': 0, '14': 0, '15': 0, '16': 0, '17': 0, '18': 0,
                      '19': 0, '20': 0, '21': 0, '22': 0, '23': 0, '24': 0, '25': 0}
    label_failed_dict = {'01': 0, '02': 0, '03': 0, '04': 0, '05': 0, '06': 0, '07': 0, '08': 0, '09': 0, '10': 0, '11': 0, '12': 0, '13': 0, '14': 0, '15': 0, '16': 0, '17': 0, '18': 0,
                         '19': 0, '20': 0, '21': 0, '22': 0, '23': 0, '24': 0, '25': 0}

    storage_path = 'X:/localUser/TrainingData/Refinement/refine_pass/'+yyyymmdd+'_정제완료'
    labeling_workload_xlsx = 'C:/Users/Jisoo/Desktop/'+str(yyyymmdd)+'_분배.xlsx'
    df = pd.read_excel(labeling_workload_xlsx)
    member_list = [df.iloc[row, 0] for row in range(len(df))]
    workload_count = [df.iloc[row, 1] for row in range(len(df))]
    workload_dict = dict(zip(member_list, workload_count))
    labeling_after = []
    for member in sorted(member_list):
        for labeling_path in sorted(glob.iglob('X:/localUser/TrainingData/Labeling/**/**/label_after/**', recursive=False)):
            if member in labeling_path and yyyymmdd in labeling_path:
                labeling_after.append(labeling_path)

    label_fin = set()
    gps = set()
    aper = set()
    classdetail = set()
    label_pass = set()
    label_pass_folder = set()
    label_pass_detail = []
    for labeling_a in labeling_after:
        for folder in sorted(os.listdir(labeling_a)):
            label_fin.add(labeling_a+'/'+folder)
            label_fin_dict[folder[:2]] += 1
            if re.findall(r'^[0-2][0-9]_X[0-9][0-9][0-9]_C[0-9][0-9][0-9]_[0-1][0-9][0-3][0-9]$', folder):
                jpg_cnt = 0
                json_cnt = 0
                for file in sorted(os.listdir(labeling_a+'/'+folder)):
                    if file.endswith('Json'):
                        json_path = labeling_a + '/' + folder+'/' + file
                        jfile = open(json_path, 'rt', encoding='UTF-8')
                        jdata = json.load(jfile)
                        jfile.close()
                        bound_cnt = int(jdata['BoundingCount'])
                        for i in range(1):
                            if len(jdata['GPS']) < 5:  # 주로 누락되는 필수값1
                                gps.add(labeling_a+'/'+folder)
                                pass
                            elif len(jdata['Aperture values']) < 1:  # 주로 누락되는 필수값2
                                aper.add(labeling_a+'/'+folder)
                                pass
                            # 01_a는 맞지만, 02_a는 틀림
                            elif jdata['Bounding'][i]['DETAILS'] not in class_dict[folder[:2]]:
                                classdetail.add(labeling_a+'/'+folder)
                                pass
                            else:
                                json_cnt += 1  # 상기 조건을 전부 만족해야 적합으로 간주
                    elif file.endswith('jpg'):
                        jpg_cnt += 1
                if jpg_cnt != json_cnt:
                    pass
                else:
                    label_pass.add(labeling_a+'/'+folder)
                    label_pass_folder.add(folder)
                    label_pass_dict[folder[:2]] += 1  # 1차 검수 통과 수량 - 폴더 단위
                    for file in sorted(os.listdir(labeling_a+'/'+folder)):
                        if file.endswith('jpg'):
                            # 1차 검수 통과 수량 - 파일 단위
                            label_jpg_dict[file[:2]] += 1
                    cnt = 0
                    for file in sorted(os.listdir(labeling_a+'/'+folder)):
                        if file.endswith('Json'):
                            cnt += 1
                            if cnt > 1:
                                break
                            json_path = labeling_a + '/' + folder+'/' + file
                            jfile = open(json_path, 'rt', encoding='UTF-8')
                            jdata = json.load(jfile)
                            jfile.close()
                            for i in range(1):
                                # 상세항목별 수량 집계를 위한 데이터 전처리
                                label_pass_detail.append(
                                    folder[:2]+'_'+jdata['Bounding'][i]['DETAILS'])

    label_pass_xlsx = 'data/label_pass/' +         str(yyyymmdd) + '_label_pass_list.xlsx'  # 요청 양식을 따르는 엑셀 파일 생성
    writer = pd.ExcelWriter(label_pass_xlsx, engine='xlsxwriter')

    p_list = set(label_pass_list)
    f_list = set(label_fin_list)
    failed = f_list - p_list
    for f in sorted(failed):
        label_failed_dict[f[61:63]] += 1

    data = {str(yyyymmdd)+'_가공완료_건': label_fin_dict.values(),
            str(yyyymmdd)+'_적합_건': label_pass_dict.values(),
            str(yyyymmdd)+'_적합_장': label_jpg_dict.values(),
            str(yyyymmdd)+'_부적합_건': label_failed_dict.values()}

    df3 = pd.DataFrame.from_dict(data, orient='index', columns=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18',
                                                                '19', '20', '21', '22', '23', '24', '25']).transpose()
    df3.to_excel(writer, sheet_name=str(yyyymmdd)+'_가공수량')

    df4 = pd.DataFrame(label_fin_list, columns=[str(yyyymmdd)+'_가공완료_list'])
    df4.index = df4.index + 1
    df4.to_excel(writer, sheet_name=str(yyyymmdd)+'_가공완료_list')

    df5 = pd.DataFrame(label_pass_list, columns=[str(yyyymmdd)+'_적합_list'])
    df5.index = df5.index + 1
    df5.to_excel(writer, sheet_name=str(yyyymmdd)+'_적합_list')

    df6 = pd.DataFrame(failed, columns=[str(yyyymmdd)+'_부적합_list'])
    df6.index = df6.index + 1
    df6.to_excel(writer, sheet_name=str(yyyymmdd)+'_부적합_list')

    for i1 in label_pass_detail:
        count_dict_box[i1] += 1

    df1 = pd.DataFrame.from_dict(count_dict_box, orient='index', columns=[
                                 str(yyyymmdd)+'_상세항목'])
    df1.to_excel(writer, sheet_name=str(yyyymmdd)+'_상세항목')

    df2 = pd.DataFrame.from_dict(label_fin_dict, orient='index', columns=[
                                 str(yyyymmdd)+'_가공완료'])
    df2.to_excel(writer, sheet_name=str(yyyymmdd)+'_가공완료')

    writer.save()

    # 필수값 누락 데이터: 정제 이관

    def _copyfileobj_faster(fsrc, fdst, length=16 * 1024 * 1024):
        while 1:
            buf = fsrc.read(length)
            if not buf:
                break
            fdst.write(buf)

    shutil.copyfileobj = _copyfileobj_faster

    # 필수값 누락 유형별로 경로 구분, 파일 복사
    # 필수값 누락된 사진을 EXIFTAGS를 이용하여 가공 전에 충분히 필터링 가능하나, 시간이 오래 걸리므로 라벨링 완료 후 필수값을 입력한다.

    path_gps = 'C:/Users/Jisoo/Desktop/error_data/error_gps'
    path_aper = 'C:/Users/Jisoo/Desktop/error_data/error_aper'
    path_class = 'C:/Users/Jisoo/Desktop/error_data/error_class'

    for g in gps:
        try:
            print(g, path_gps+'/'+g[-17:])
            shutil.copytree(g, path_gps+'/'+g[-17:])
        except FileExistsError:
            pass
        except FileNotFoundError:
            pass

    for a in aper:
        try:
            print(a, path_aper+'/'+a[-17:])
            shutil.copytree(a, path_aper+'/'+a[-17:])
        except FileExistsError:
            pass
        except FileNotFoundError:
            pass

    for c in classdetail:
        try:
            print(c, path_class+'/'+c[-17:])
            shutil.copytree(c, path_class+'/'+c[-17:])
        except FileExistsError:
            pass
        except FileNotFoundError:
            pass

    # 필수값 누락 파일 목록 생성

    refine_return = set()
    for folder in sorted(os.listdir(path_gps)):
        refine_return.add(path_gps+'/'+folder)

    for folder in sorted(os.listdir(path_aper)):
        refine_return.add(path_aper+'/'+folder)

    for folder in sorted(os.listdir(path_class)):
        refine_return.add(path_class+'/'+folder)

    refine_return = pd.DataFrame(refine_return, columns=[
                                 str(yyyymmdd)+'_refine_return'])
    refine_return_xlsx = 'data/refine_return/' +         str(yyyymmdd)+'_refine_return_list.xlsx'

    with pd.ExcelWriter(refine_return_xlsx, engine='xlsxwriter') as writer:
        refine_return.index = refine_return.index + 1
        refine_return.to_excel(
            writer, sheet_name=str(yyyymmdd)+'_refine_return')
        writer.book.use_zip64()
        writer.save()

    def labeling_validation_error_mail(yyyymmdd):
        smtp = smtplib.SMTP('smtp.gmail.com', 587)
        smtp.ehlo()
        smtp.starttls()
        smtp.login('mymail@gmail.com', 'mypassword')

        refine_mail = ['refine_mail@gmail.com']  # 정제 담당자 메일 주소
        msg = MIMEMultipart()
        msg['Subject'] = str(yyyymmdd)+'_필수값 누락'  # 제목
        part = MIMEText(str(yyyymmdd)+'일자 필수값 누락 폴더 목록입니다.')  # 내용
        msg.attach(part)

        filepath = refine_return_xlsx

        with open(filepath, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment', filename=filepath)
            msg.attach(part)
        msg

        for address in refine_mail:
            msg['To'] = address
            smtp.sendmail('mymail@gmail.com', address, msg.as_string())
            print(address)

    def labeling_validation_notice(yyyymmdd):
        def post_message(token, channel, text):
            response = requests.post('https://slack.com/api/chat.postMessage',
                                     headers={
                                         'Authorization': 'Bearer' + token},
                                     data={'channel': channel, 'text': text}
                                     )
            print(response)

        myToken = 'my-slack-token-000000000000000000000000'

        post_message(myToken, '#notice_me', str(yyyymmdd) +
                     '_validation after labeling_완료')

