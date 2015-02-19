__author__ = 'amit'

from sql_wrapper.sqlWrapper import SQLWrapper
import logging, json, html
from pmsi_utility.utility import helper as um
from pylastic import ESWrapper,ESQueryBuilder
import uuid, time
from PandaWrapper import PylasticPanda
from PandaWrapper import JsonToPanda as jp


import arrow
from mongo_wrapper.mongoWrapper import MongoWrapper
import hashlib, re

logger = logging.getLogger('PromoDataWorker')
um.setup_logger('c:\\temp\\', 'PromoDataWorker', logger)
es = ESWrapper(['pmsi0087'], logger)
panda = PylasticPanda(['pmsi0087'], logger)

mongo = MongoWrapper("67.202.79.14", 27017)
# server name in {},
sql = SQLWrapper('{PMSISQL06}','Retailography_Dev', True, None, None, logger)

def get_csv():
    response = panda.get_all_data('shirland_promo_data_from_sql', 'primary')
    print(len(response))
    response.to_csv('shirland_retailography_dev_data.csv')





def get_fields(specified_fields, sample_data):
    """
    gets the list of defined and dynamic fields from sample set
    """
    fields = specified_fields

    for key in sample_data.keys():
        found = False
        for field in fields:
            if field['name'] == key:
                found = True
                break;

        if not found:
            f1 = {"name": key, "type" : "string"}
            fields.append(f1)
    return fields


def cleanup(text):

    text = str(um.remove_foreign_chars_pattern(text, '[^a-zA-Z0-9\-_]'))

    if text is None:
        return text

    text = text.replace(' ', '')
    text = text.replace(',', '')
    return text


def cleanup_text(text):
    cleaned = "".join(re.findall('[a-zA-Z0-9_\-]', text))
    return cleaned

def cleanup_text_test(text):
    cleaned = "".join(re.findall("weight\(\(?<name>[^:]*\)", text))
    return cleaned

def sync_base_data_promo_lines():

    text ='weight(zip:14005 in 14055) [PerFieldSimilarity], result of:\' (86968448)'
    cltext = cleanup_text_test(text)
    rows = sql.get_objects_for_qry("SELECT * FROM [Retailography_Dev].[dbo].[Base_Data_Promo_Lines] WHERE SweepId Is Not NULL ")

    json_rows = []
    logger.info("Rows Found - %s", len(rows))
    for r in rows:
        try:
            if r['SweepId'] is None:
                r['SweepId'] = 999999999

            if r['UniqueIdent'] is None:
                r['UniqueIdent'] = 'Unknown'

            r['CreatedOn'] = arrow.get(str(r['CreatedOn']), 'YYYY-MM-DD HH:mm:ss').format('YYYY-MM-DDTHH:mm:ss')
            t = time.strftime('YYYY-MM-DDTHH:mm:ss')
            ###Product_ID_Title = cleanup_text(r['Title'])
            ###Product_ID_UniqueIdent = cleanup_text(r['UniqueIdent'].replace('.0','').replace('_'+str(r['SweepId']),''))

            Generated_Site_Product_ID = str
            if r['Retail Brand'] == 'Aldi':
                Generated_Site_Product_ID = cleanup_text(r['Title'])
            else:
                site_name_to_remove = '_'+str(r['Site'].replace('.',''))
                Generated_Site_Product_ID = cleanup_text(r['UniqueIdent'].replace(site_name_to_remove,'').replace('_'+str(r['SweepId']),''))

            if Generated_Site_Product_ID == r['Site_product_id']:
                r['Matching'] = 'True'
            else:
                r['Matching'] = 'False'

            r['Generated_Site_Product_ID'] = Generated_Site_Product_ID
            r['Generated_Site_Product_ID_Hash'] = hashlib.md5(r['Generated_Site_Product_ID'].encode('utf-8')).hexdigest()

            r['Generated_Product_ID'] = r['Generated_Site_Product_ID'] + '_'+ cleanup_text(r['Site'])
            r['Generated_Product_ID_Hash'] = hashlib.md5(r['Generated_Product_ID'].encode('utf-8')).hexdigest()

            r['Generated_Document_ID'] = r['Generated_Product_ID' ]+ '_'+ cleanup_text(str(r['SweepId']))
            r['Generated_Document_ID_Hash'] = hashlib.md5(r['Generated_Document_ID'].encode('utf-8')).hexdigest()

            r['GeneratedGUID'] = uuid.uuid4().__str__()
            ###mongo.insert_data('promo_id_match', 'promo_id_matching_retail_from_sql',r)

            json_rows.append(json.dumps(str(r)))
        except Exception as ex:
                logger.error(str(ex))
    es.bulk_index(es.es_client, 'shirland_promo_data_from_sql', 'primary', rows, 'GeneratedGUID')
    get_csv()


def sync_product_master():
    rows = sql.get_objects_for_qry("SELECT  * FROM [Retailography_Dev].[dbo].[product_master] ")

    logger.info("Rows found in Product_Master - %s", len(rows))
    json_rows = []
    logger.info("Starting jsonification...")
    for r in rows:
        try:
            Product_ID_Title = cleanup(r['Title'])

            r['Product_ID_Title'] = Product_ID_Title
            r['Product_ID_Title_Hash'] = hashlib.md5(r['Product_ID_Title'].encode('utf-8')).hexdigest()

            r['Site_Product_ID_Title'] = r['Product_ID_Title'] + '_'+ cleanup(r['Site'])
            r['Site_Product_ID_Title_Hash'] = hashlib.md5(r['Site_Product_ID_Title'].encode('utf-8')).hexdigest()

            json_rows.append(json.dumps(str(r)))
        except Exception as ex:
            logger.error(str(ex))

    logger.info("Starting indexing...")
    es.bulk_index(es.es_client, 'shirland_promo_data', 'product_master', rows, 'Site_Product_ID_Title_Hash')


def sync_product_category_lookup():
    rows = sql.get_objects_for_qry("SELECT  * FROM [Retailography_Dev].[dbo].[Product_category_lookup] ")

    logger.info("Rows found in Product_category_lookup - %s", len(rows))
    json_rows = []
    logger.info("Starting jsonification...")
    for r in rows:
        try:
            r['CatId'] = str(r['Category ID'])
            json_rows.append(json.dumps(str(r)))
        except Exception as ex:
            logger.error(str(ex))

    logger.info("Starting indexing...")
    es.bulk_index(es.es_client, 'shirland_promo_data', 'product_category_lookup', rows, 'CatId')


sync_base_data_promo_lines()

###cleanup('MAÎTRE DE FRANCE
