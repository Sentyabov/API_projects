import requests
import json
import pandas as pd
import datetime
import VitalityBooster as vb
import DataBase as db
ids_list = []
names_list = []
phone_list = []
date_list = []
leads_id = []
leads_company = []
pages_list = []

select_token = '''select auth_token
		                from zip_services.api_accesses aa'''
update_token = """update zip_services.api_accesses set auth_token = '{refresh_token}', update_date = '{refresh_token_update}' 
                    where api_sourse_name = 'amo_crm'"""
insert_token = '''insert into zip_services.api_accesses 
                    (api_sourse_name, api_type, auth_token, update_date)
                        values {insert_here}'''
insert_data = '''insert into other_crm.amo_crm (id_amo, full_name, phone_number, updated_at, lead_source, page)
                        VALUES  {insert_here}'''
update_data = '''select max(ac.page::int) as max_page,
		                max(ac.updated_at) as last_update
	                        from other_crm.amo_crm ac'''


def names(json):
    k = json['_embedded']['contacts']
    for i in k:
        names_list.append(i['name'])


def ids(json):
    k = json['_embedded']['contacts']
    for i in k:
        ids_list.append(i['id'])


def phone_number(json, page):
    k = json['_embedded']['contacts']
    for i in k:
        if i['custom_fields_values'] is not None:
            if i['custom_fields_values'][0]['values'][0]['value'].isdigit():
                phone_list.append(i['custom_fields_values'][0]['values'][0]['value'])
            else:
                phone_list.append('No number')
        else:
            phone_list.append('No number')
        pages_list.append(page)


def updated_at(json):
    k = json['_embedded']['contacts']
    for i in k:
        if len(str(i['updated_at'])) == 10:
            date_list.append(pd.to_datetime(i['updated_at'], unit='s'))
        else:
            date_list.append(pd.to_datetime(i['updated_at'], unit='ms'))


def leads(json):
    k = json['_embedded']['contacts']
    for i in k:
        if len(i['_embedded']['leads']) == 0:
            leads_id.append(None)
            continue
        else:
            leads_id.append(i['_embedded']['leads'][0]['id'])


def leads_company_f(json):
    k = json['_embedded']['tags']
    if len(k) == 0:
        leads_company.append(None)
    else:
        leads_company.append(k[0]['name'])


if __name__ == '__main__':
    datalake = vb.MessengerSQL(db.PostgreSQL_Datalake())
    datalake.connect()
    df = datalake.send_command(select_token)
    df_page = datalake.send_command(update_data)
    refresh_token = (df['auth_token'].iloc[0])
    data = {'client_id': '0000a9cc-9575-4c80-81d8-b102c5b63459',
            'client_secret': 'koLsceOjXgu8iVMvTQqYsk5VmVWlT7Q5MFE1s7tuqIbCrhdQ8KfguaWxluEraWpp',
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'redirect_uri': 'https://drive.google.com/drive/folders/1ih0nfxDdU0CJ0FlVf3i_XS-lNKHT6sJe?usp=sharing'}
    new_url = 'https://sibset.amocrm.ru/oauth2/access_token'
    request = requests.post(new_url, data=data)
    request_dict = json.loads(request.text)
    refresh_token = request_dict['refresh_token']
    refresh_token_update = str(datetime.datetime.now())
    data_token = {'api_source_name': ['amo_crm'],
                  'api_type': ['REST'],
                  'auth_token': [refresh_token],
                  'update_date': [refresh_token_update]}
    token_df = pd.DataFrame(data_token)
    access_token = request_dict['access_token']
    datalake.send_command_no_data(update_token.format(refresh_token=refresh_token, refresh_token_update=refresh_token_update))
    api_call_headers = {'Authorization': 'Bearer ' + access_token}
    api_statuses_response_contacts = requests.get(
        f'https://sibset.amocrm.ru/api/v4/contacts?page=1&limit=1&with=leads',
        headers=api_call_headers, verify=True)
    page = int((df_page['max_page'].iloc[0]))
    update_time = (df_page['last_update'].iloc[0])
    while api_statuses_response_contacts.status_code != 204:
        api_statuses_response_contacts = requests.get(
            f'https://sibset.amocrm.ru/api/v4/contacts?page={page}&limit=250&with=leads',
            headers=api_call_headers, verify=True, timeout=15, allow_redirects=False)
        if api_statuses_response_contacts.status_code == 204:
            break
        api_statuses_response_json = api_statuses_response_contacts.json()
        ids(api_statuses_response_json)
        names(api_statuses_response_json)
        phone_number(api_statuses_response_json, page)
        updated_at(api_statuses_response_json)
        leads(api_statuses_response_json)
        page += 1
    for i in leads_id:
        if i is not None:
            api_statuses_response_leads = requests.get(
                f'https://sibset.amocrm.ru/api/v4/leads/{i}',
                headers=api_call_headers, verify=True, timeout=15, allow_redirects=False)
            api_leads = api_statuses_response_leads.json()
            leads_company_f(api_leads)
        else:
            leads_company.append(None)
    index = 0
    for i in names_list:
        if i.find('W11') != -1 or i.find('whatsapp') != -1:
            names_list[index] = None
            if i.find('whatsapp') != -1:
                phone_list[index] = i[4:15]
                leads_company[index] = 'WhatsApp'
            else:
                phone_list[index] = 'No number'
                leads_company[index] = None
        index += 1
    data_contacts = {'id_amo': ids_list,
                     'full_name': names_list,
                     'phone_number': phone_list,
                     'updated_at': date_list,
                     'lead_source': leads_company,
                     'page': pages_list}
    df = pd.DataFrame(data_contacts)
    for i in range(len(df.phone_number)):
        df.phone_number[i] = ''.join(filter(str.isalnum, df.phone_number[i]))
    df = df.loc[~df['full_name'].isin(['79039331666, входящий успешный'])]
    df = df.loc[~df['phone_number'].isin(['No number', 'v.melnikov@sibset-team.ru', 'Nonumber'])]
    df = df.loc[df['updated_at'] > update_time]
    datalake.execute_method(insert_data, df)
