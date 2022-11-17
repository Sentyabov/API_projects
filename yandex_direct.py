
# -*- coding: utf-8 -*-



import requests
import re
import json
import pandas as pd
import sys
import io
import DataBase as db
import pymysql
import time
import VitalityBooster as vb

dict_of_indexes = {'abakan': 'Абакан',  'ach': 'Ачинск', 'angero': 'Анжеро-Суженск',
                    'barnaul': 'Барнаул', 'belovo': 'Белово', 'berdsk': 'Бердск', 'biisk' : 'Бийск', 'biysk': 'Бийск',
                    'gramot': 'Грамотеино', 'gurevsk': 'Гурьевск',
                    'divno': 'Дивногорск', 'zel': 'Зеленогорск', 'igarka': 'Игарка', 'inskoy': 'Инской',
                    'lesosibirsk': 'Лесосибирск', 'mezhdurechensk': 'Междуреченск', 'min': 'Минусинск',
                    'molod': 'Молодёжный',
                    'myski': 'Мыски', 'naz': 'Назарово', 'nvkz': 'Новокузнецк', 'novokuznetsk': 'Новокузнецк',
                    'novosib': 'Новосибирск', 'norilsk': 'Норильск',
                    'kiselevsk': 'Киселевск', 'kemer': 'Кемерово', 'kemerovo': 'Кемерово', 'krasn': 'Красноярск',
                    'krasnoyarsk': 'Красноярск',
                    'osinniki': 'Осинники', 'prokopevsk': 'Прокопьевск', 'topki': 'Топки', 'chernogorsk': 'Черногорск'}
list_of_cities = []
#Проверка системы и установки кодировки

if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b''):
            return x.decode('utf8')
        else:
            return x






def get_sources_ac_name(text):
    if ('search' in text):
        return ('yandex.search')
            
    elif ('media' in text):
        return ('yandex.media')
            
    elif ('retargeting' in text):
        return ('yandex.retargeting')

    elif ('net' in text):
        return ('yandex.net')
            

def list_to_string(array):
    str1 = ""
    array = list(array)
    for i in range(len(array)):
        
        if i != (len(array)-1):
            
            if (array[i]) == pymysql.NULL:
                str1 = str1 + (str(array[i]) + ", ")
            else:
                 str1 = str1 + ("'" + str(array[i]) + "', ")
                
        else:
            if (array[i]) == pymysql.NULL:
                 str1 = str1 + (str(array[i]))
            else:
                 str1 = str1 + ("'" + str(array[i]) + "'")

    return (str1)


def set_datalake(df):
    datalake = vb.MessengerSQL(db.PostgreSQL_Datalake())
    datalake.connect()
    insert = """insert into bitrix.yandex_direct_ads 
                            (date, 
                            campaign_name, 
                            cities,
                            sources_ac_name, 
                            impressions, 
                            clicks, 
                            cost_per_conversion, 
                            ctr, 
                            cost)
        values {insert_here}"""
    datalake.execute_method(insert, df)



def error_processing(isError, textError):
    datalake = vb.MessengerSQL(db.PostgreSQL_Datalake())
    datalake.connect()
    if isError == True:
        services = db.Services_Scripts()
        postgre_error = db.PostgreSQL_Datalake()
        conn_pg_error = postgre_error.set_connections()
        services.services_failure('yandex_direct', conn_pg_error, textError)
    else:
        vb.send_successfully('yandex_direct')



def main():
    ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'
    token = 'AQAAAAAzI2G_AAMfZX7i0GRUGkX7suiUrdg51v8'

    #AQAAAAAzI2G_AAeEEEWoInPvcUhjpMRC4F4gVvc
    yandexLogin = 'sibset-dl'
    headers = {
            # OAuth-токен. Использование слова Bearer обязательно
            "Authorization": "Bearer " + token,
            # Логин клиента рекламного агентства
            "Client-Login": yandexLogin,
            # Язык ответных сообщений
            "Accept-Language": "ru",
            # Режим формирования отчета
            "processingMode": "auto"

            }

    # Создание тела запроса
    # body = {
    #         "params": {
    #             "SelectionCriteria": {
    #                 "DateFrom": "2019-06-27",
    #                 "DateTo": "2022-06-26",
    #             },
    #             "FieldNames": [
    #                 "Date",
    #                 "CampaignName",
    #                 "Impressions",
    #                 "Clicks",
    #                 "CostPerConversion",
    #                 "Ctr",
    #                 "Cost",
    #             ],
    #             "ReportName": ("Report_"+str(int(time.time()))),
    #             "ReportType": "CUSTOM_REPORT",
    #             "DateRangeType": "CUSTOM_DATE",
    #             "Format": "TSV",
    #             "IncludeVAT": "NO",
    #             "IncludeDiscount": "NO"
    #         }
    #     }

    
    body = {
        "params": {
            "SelectionCriteria": {
            },
            "FieldNames": [
                "Date",
                "CampaignName",
                "Impressions",
                "Clicks",
                "CostPerConversion",
                "Ctr",
                "Cost",
                'CampaignUrlPath'
            ],
            "ReportName": ("Report_"+str(int(time.time()))),
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "YESTERDAY",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    
    



    # Кодирование тела запроса в JSON
    body = json.dumps(body, indent=4)

    #            "DateFrom": "2021-06-01",
    #              "DateTo": "2021-11-24",

    # --- Запуск цикла для выполнения запросов ---
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросыe
    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                error_processing(True, ("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди"))
                break
            elif req.status_code == 200:
                format(u(req.text))
                #error_processing(True, format(u(req.text)))
                break
            elif req.status_code == 201:
                print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                time.sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                time.sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                error_processing(True, ("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее"))
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                error_processing(True, ("Время формирования отчета превысило серверное ограничение."))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                error_processing(True, ("Произошла непредвиденная ошибка")+ ("-- RequestId:  {}".format(req.headers.get("RequestId", False))) )
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            error_processing(True, ("Произошла ошибка соединения с сервером API"))
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            error_processing(True, ("Произошла непредвиденная ошибка"))
            # Принудительный выход из цикла
            break


    #собираем датафрейм
    print(req.text)
    df = pd.read_csv(io.StringIO(req.text),header=1, sep='	', index_col=0)
    
    df['Cost'] = df['Cost']/1000000


    for i in range(len(df['CostPerConversion'])-1):
        if df['CostPerConversion'][i] != '--':
            df['CostPerConversion'][i] = int(df['CostPerConversion'][i])/1000000
        else:
            df['CostPerConversion'][i] = 0
    df = df.sort_values(by=['Clicks'], ascending=False)
    df.insert(0, 'sources_ac_name', 'yandex')
    df = df.dropna(subset=['CampaignName'])

    for i in range(len(df['CampaignName'])):
        df['sources_ac_name'][i] = str(get_sources_ac_name(df['CampaignName'][i]))
    #print (df.columns)
    columns_new = ['sources_ac_name', 'CampaignName', 'Cities', 'Impressions', 'Clicks',
       'CostPerConversion', 'Ctr', 'Cost', 'CampaignUrlPath']
    df_copy = df
    for i in df_copy.CampaignName:
        i = re.sub(r'[\d]+', r'', i)
        str_list = i.split('_')
        temporary_list = []
        for j in str_list:
            if j in dict_of_indexes.keys():
                temporary_list.append(dict_of_indexes[j])
        # print(i, temporary_list)
        list_of_cities.append(temporary_list)
    for i in list_of_cities:
        # print(len(i))
        if len(i) == 0:
            i = None
    df['Cities'] = list_of_cities
    df = df.reindex(columns=columns_new)
    df.to_csv(r'test_task.txt')

    df = df[['CampaignName', 'Cities', 'sources_ac_name', 'Impressions', 'Clicks', 'CostPerConversion', 'Ctr', 'Cost',
             'CampaignUrlPath']]
     
    df = df.reset_index()
    # set_datalake(df)
    
    # error_processing(False, None)

main()




