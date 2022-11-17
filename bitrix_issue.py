import DataBase as db
import VitalityBooster as vb



bitrix_get = '''SELECT f.id_bitrix, 
    bie.TIMESTAMP_X as active_time_begin, 
    bie.DATE_CREATE as create_date, 
    bie.NAME as title, 
    c.city, 
    f.person_name, 
    p.phone_number, 
    lb.landing_block, 
    tn.tariff_name, 
    eq.equipment, 
    ie.id_issue_211_ru, 
    cam.utm_campaign, 
    src.utm_source, 
    ft.form_type, 
    con.utm_content, 
    ter.utm_term 
      FROM (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
          case 
         when IBLOCK_ELEMENT_ID in (SELECT IBLOCK_ELEMENT_ID 
                   FROM b_iblock_element_property 
              where IBLOCK_PROPERTY_ID = 70) 
       then VALUE 
        else NULL end as person_name 
           FROM b_iblock_element_property biep 
         where biep.IBLOCK_ELEMENT_ID >= 288 
   group by IBLOCK_ELEMENT_ID) f 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as city 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 75) c on c.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as phone_number 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 71) p on p.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as landing_block 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 74) lb on lb.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         concat(bie.NAME, ' [', biep.VALUE, ']') as tariff_name 
          FROM b_iblock_element_property biep 
      left join b_iblock_element bie on bie.ID = biep.VALUE 
   where IBLOCK_PROPERTY_ID = 77) tn on tn.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         concat(bie.NAME, ' [', biep.VALUE, ']') as equipment 
          FROM b_iblock_element_property biep 
   left join b_iblock_element bie on bie.ID = biep.VALUE 
   where IBLOCK_PROPERTY_ID = 78) eq on eq.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as id_issue_211_ru 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 82) ie on ie.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as utm_campaign 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 87) cam on cam.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as utm_source 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 86) src on src.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         case 
          when VALUE = 68 then 'Заказать звонок' 
          when VALUE = 67 then 'Оформить заявку' 
         else NULL end as form_type 
          FROM b_iblock_element_property 
        where IBLOCK_PROPERTY_ID = 76 
          group by IBLOCK_ELEMENT_ID) ft on ft.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as utm_content 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 88) con on con.id_bitrix = f.id_bitrix 
left join (SELECT IBLOCK_ELEMENT_ID as id_bitrix, 
         VALUE as utm_term 
          FROM b_iblock_element_property 
         where IBLOCK_PROPERTY_ID = 89) ter on ter.id_bitrix = f.id_bitrix 
left join b_iblock_element bie on bie.ID = f.id_bitrix 
 where (length(c.city) < 31 or c.city is null) 
   and (length(f.person_name) < 101 or f.person_name is null) 
   and (length(p.phone_number) < 21 or p.phone_number is null) 
   and (length(lb.landing_block) < 201 or lb.landing_block is null) 
   and (length(tn.tariff_name) < 101 or tn.tariff_name is null) 
   and (length(eq.equipment) < 101 or eq.equipment is null) 
   and (length(cam.utm_campaign) < 51 or cam.utm_campaign is null) 
   and (length(src.utm_source) < 31 or src.utm_source is null) 
   and (length(ft.form_type) < 31 or ft.form_type is null) 
   and (length(con.utm_content) < 31 or con.utm_content is null) 
   and (length(ter.utm_term) < 101 or ter.utm_term is null)
   and (f.id_bitrix  > {max_id});'''

datalake_insert = '''INSERT INTO bitrix.issue (
                ID_BITRIX,
                ACTIVE_TIME_BEGIN,
                CREATE_DATE,
                TITLE,
                CITY,
                PERSON_NAME,
                PHONE_NUMBER,
                LANDING_BLOCK,
                TARIFF_NAME,
                EQUIPMENT,
                ID_ISSUE_211_RU,
                UTM_CAMPAIGN,
                UTM_SOURCE,
                FORM_TYPE,
                UTM_CONTENT,
                UTM_TERM
            )
            VALUES {insert_here}'''

get_max_id = '''select max(id_bitrix) as max_id
                    from bitrix.issue'''

if __name__ == '__main__':
    try:
        # connect to MySQL_Bitrix
        bitrix = vb.MessengerSQL(db.BitrixDB())
        bitrix.connect()

        # connect to DataLake
        datalake = vb.MessengerSQL(db.PostgreSQL_Datalake())
        datalake.connect()
        max_id = datalake.send_command(get_max_id)['max_id'][0]
        #print (max_id)
        df = bitrix.send_command(bitrix_get.format(max_id=max_id))
        #print(df)
        # insert dataframe
        datalake.execute_method(datalake_insert, df)
        vb.send_successfully('bitrix_issue')
    except Exception as exception:
       vb.send_error(exception, 'bitrix_issue')
