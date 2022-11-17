import VitalityBooster as vb
import DataBase as db
from pandas import DataFrame
import requests
import datetime
import re
import time
token = 'be01e4273fd5a09c1407686368e429d1d4c1a85c61b48a5b18b2b1a6ef6d58fca6c4a98fa096a9afedd88'
version = 5.131
cab_num = 1900013814
clients_list = []
insert_data = '''insert into bitrix.vk_ads_new (sources_ac_name, ads_name, campaigns_name, date, spent, impressions, clicks, ctr, utm_campaign)
                        VALUES  {insert_here}'''
update = ''' select (cast((max(date) - interval'1 min') as varchar(19))) as last_update
	                    from bitrix.vk_ads_new'''


def clients():
    r = requests.get('https://api.vk.com/method/ads.getClients', params={
        'account_id': cab_num,
        'access_token': token,
        'v': version})
    clients_data = r.json()['response']
    for person in clients_data:
        clients_list.append(person['id'])


if __name__ == '__main__':
    clients()
    datalake = vb.MessengerSQL(db.PostgreSQL_Datalake())
    datalake.connect()
    df_page = datalake.send_command(update)
    today = str(datetime.datetime.today())[:10]
    max_day = str((df_page['last_update'].iloc[0]))[:10]
    for client in clients_list:
        time.sleep(1)
        campaigns = requests.get('https://api.vk.com/method/ads.getCampaigns', timeout=5, params={
            'access_token': token,
            'account_id': cab_num,
            'client_id': int(client),
            'include_deleted': 1,
            'v': version
        })
        time.sleep(1)
        ads = requests.get('https://api.vk.com/method/ads.getAds', timeout=5, params={
            'access_token': token,
            'v': version,
            'account_id': cab_num,
            'client_id': int(client),
            'include_deleted': 1
        })
        time.sleep(1)
        add_parameters = requests.get('https://api.vk.com/method/ads.getAdsLayout', timeout=5, params={
            'access_token': token,
            'v': version,
            'account_id': cab_num,
            'client_id': int(client),
            'include_deleted': 1
        })
        data = ads.json()['response']
        data_test = add_parameters.json()['response']
        campaigns_data = campaigns.json()['response']
        sources_list = []
        ads_campaign_list = []
        ads_id_list = []
        ads_impressions_list = []
        ads_clicks_list = []
        ads_spent_list = []
        ads_url_list = []
        ads_day_start_list = []
        ads_day_end_list = []
        ads_ctr_list = []
        add_id_url_dict = {}
        ad_camp_id_dict = {}
        camp_id_name_dict = {}
        ad_id_name_dict = {}
        alpha = 0
        for i in data:
            ad_camp_id_dict[i['id']] = i['campaign_id']
            ad_id_name_dict[i['id']] = i['name']
        for i in campaigns_data:
            camp_id_name_dict[i['id']] = i['name']
        for ad in data_test:
            if ad['link_url'].find('utm_campaign') != -1:
                utm_campaign = re.findall('&utm_campaign=(.*?)(&|$)', ad['link_url'])
                add_id_url_dict[ad['id']] = utm_campaign[0][0]
            else:
                add_id_url_dict[ad['id']] = None
        for ad_id in ad_camp_id_dict:
            r = requests.get('https://api.vk.com/method/ads.getStatistics', params={
                'access_token': token,
                'v': version,
                'account_id': cab_num,
                'ids_type': 'ad',
                'ids': ad_id,
                'period': 'day',
                'date_from': f'{max_day}',
                'date_to': f'{today}'
            })
            try:
                data_stats = r.json()['response']
                for i in range(len(data_stats)):
                    for j in range(len(data_stats[i]['stats'])):
                        ads_impressions_list.append(data_stats[i]['stats'][j]['impressions'])
                        if 'clicks' in data_stats[i]['stats'][j]:
                            ads_clicks_list.append(data_stats[i]['stats'][j]['clicks'])
                        else:
                            ads_clicks_list.append(0)
                        if 'spent' in data_stats[i]['stats'][j]:
                            ads_spent_list.append(data_stats[i]['stats'][j]['spent'])
                        else:
                            ads_spent_list.append(0)
                        ads_day_start_list.append(data_stats[i]['stats'][j]['day'])
                        ads_id_list.append(data_stats[i]['id'])
                        ads_campaign_list.append(ad_camp_id_dict[ad_id])
                        alpha_list = [0, None]
                        if not ads_impressions_list[alpha] in alpha_list:
                            ads_ctr_list.append('{:.2f}'.format(ads_clicks_list[alpha] / ads_impressions_list[alpha] * 100))
                        else:
                            ads_ctr_list.append(0)
                        alpha += 1
            except KeyError:
                continue
        for i in range(len(ads_id_list)):
            if ads_id_list[i] in add_id_url_dict.keys():
                ads_url_list.append(add_id_url_dict[ads_id_list[i]])
        for i in range(len(ads_id_list)):
            if ads_id_list[i] in ad_id_name_dict.keys():
                ads_id_list[i] = ad_id_name_dict[ads_id_list[i]]
        for i in range(len(ads_campaign_list)):
            if ads_campaign_list[i] in camp_id_name_dict.keys():
                ads_campaign_list[i] = camp_id_name_dict[ads_campaign_list[i]]
        for i in range(len(ads_id_list)):
            sources_list.append('vkontakte')
        df = DataFrame()
        df['sources_ac_name'] = sources_list
        df['ads_name'] = ads_id_list
        df['campaigns_name'] = ads_campaign_list
        df['day'] = ads_day_start_list
        df['spent'] = ads_spent_list
        df['impressions'] = ads_impressions_list
        df['clicks'] = ads_clicks_list
        df['ctr'] = ads_ctr_list
        df['utm_campaign'] = ads_url_list
        datalake.execute_method(insert_data, df)
