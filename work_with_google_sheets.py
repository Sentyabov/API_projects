import gspread
import pandas as pd
import datetime
import DataBase
import VitalityBooster as vb
name_module = 'work_with_google_sheets'

try:
    insert = '''insert into static.contacts_from_hypotheses (loading_date, receiving_date, name, email, phone_number,
                account_number, source, hypothesis_name, is_subscriber) values {insert_here}'''
    # authenticate Google service account
    gp = gspread.service_account(filename='pythonanalytics-333309-f1b7b49bd63e.json')

    # open Google spreadsheet
    sh = gp.open_by_key('11-5y7dvihPu5hhnbtQBQ4svIQo0IndX_KcGIoDk0EPs')

    # select worksheet
    wsheet = sh.worksheet('Лист1')

    # getting values
    all_rows = wsheet.get_all_values()
    columns = all_rows.pop(0)

    # correcting account_number
    for row in all_rows:
        if len(row[4]) < 13:
            while len(row[4]) != 12:
                row[4] = '0' + row[4]
            row[4] = '2' + row[4]
        if len(row[4]) > 13:
            while len(row[4]) != 13:
                if row[4][0] == '2' and row[4][1] == '0':
                    row[4] = row[4].replace('0', '', 1)
                else:
                    row[4] = ''

    # correcting e-mails with one mistake
    right_emails = ['rambler.ru', 'gmail.com', 'mail.ru', 'yandex.ru', 'bk.ru', 'list.ru', 'inbox.ru',
                    'internet.ru', 'academ.org', 'ngs.ru', 'ya.ru', 'yahoo.com', 'hotmail.com']

    for row in all_rows:
        domain = row[2].split('@')[1]
        for item in right_emails:
            if abs(len(domain) - len(item)) <= 1:
                counter = 0
                for letter in domain:
                    if item.find(letter) != -1:
                        counter += 1
                if abs(counter - len(item)) <= 1:
                    domain = item
        row[2] = row[2].split('@')[0] + '@' + domain

    df = pd.DataFrame(all_rows, columns=columns)
    df = df.replace(r'^\s*$', None, regex=True)
    df.loc[df['Лицевой счет'].str.isdigit() == False, ['Лицевой счет']] = None

    # getting current date
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    df.insert(0, 'Дата загрузки', today)
    df = df.drop_duplicates()
    datalake = vb.MessengerSQL(DataBase.PostgreSQL_Datalake())
    datalake.connect()
    datalake.execute_method(insert, df)

    wsheet.clear()
    wsheet.append_row(['Дата получения', 'Имя', 'E-mail', 'Номер телефона', 'Лицевой счет',
                       'Источник получения', 'Название гипотезы', 'Является абонентом'])
    vb.send_successfully(name_module)
except Exception as err:
    vb.send_error(err, name_module)
