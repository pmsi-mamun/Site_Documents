__author__ = 'mrashid'
import sys, os, csv, json, datetime, re
from mongo_wrapper.mongoWrapper import MongoWrapper
from sql_wrapper.sqlWrapper import SQLWrapper
from pylastic import ESWrapper
from pmsi_utility.utility import helper as um
#coding=UTF8
import codecs, logging
logger = logging.getLogger('DataDownloader')
um.setup_logger('c:\\temp\\', 'DataDownloader', logger)

#sql = SQLWrapper('pmsisql02', 'ScrapingCosmos', False, 'Scraping_Bot_User', 'Scraping_Bot_User', logger)
es = ESWrapper("Pmsi0087:9200",logger)
#es = ESWrapper("84.40.63.146:9200",logger)
mgo = MongoWrapper("mongodb://173.248.182.102/?w=1",27017)
file_header = []


def get_data_for_category_sweeps():
    print("Getting today's 'CategoryScraper' sweeps... ")
    #sweeps = sql.get_todays_completed_sweeps('CategoryScraper')

    #d = mgo.get_all_data_from_collection('location_data_jun','aldinord_stores')

    sweeps = [14499]
    print("Sweeps Found -> " + str(len(sweeps)))
    data = None
    fieldName='sweepid'
    alldata = []

    for sweep in sweeps:
        query = '{"query":{"term":{"sweepid":{"value":'+ str(sweep)+'}}}}'

        #response = es.get_data_for_qry_raw('shirland_product_data', 'promotion_data', json.loads(query))
        #response = es.get_data_for_qry_raw('shirland_store_data', 'kaufland_stores', json.loads(query))
        #response = es.get_data_for_qry_raw('lidl_co_uk_postcode_promot', 'lidl_co_uk_postcode_promot', json.loads(query))
        #response = es.get_data_for_qry_raw('kaufland_stores_promo_data', 'kaufland_de_stores_promo_data', json.loads(query))
        #response = es.get_data_for_qry_raw('lidl_de_postcode_promo', 'lidl_de_postcode_promo', json.loads(query))
        #response = es.get_data_for_qry_raw('lidl_de_postcode_promo', 'lidl_de_postcode_promo', json.loads(query))
        #data = mgo.get_data_for_field('sh_version_test','sh_version_test_store','sweepid', sweep)
        response = es.get_data_for_qry_raw('shirland_product_data_fr_promo', 'promotion_data', json.loads(query))

        if response is not None:
            data = response['source']
        else:
            continue

        for d in data:
            alldata.append(d)
            build_file_header(d)

        print("Data found for sweep - "+str(sweep) + " --> " + str(len(data)))
    return alldata

"""
def get_data_for_category_sweeps():
    print("Getting today's 'CategoryScraper' sweeps... ")
    data = None
    fieldName='sweepid'
    alldata = []

    response = es.get_all_data('aldi_es_geo_data_feb','ES')
    if response is not None:
        data = response['source']
    else:
        return None
    for d in data:
        alldata.append(d)
        build_file_header(d)

    return alldata

"""

def build_file_header(raw):
    global file_header
    for k,v in raw.items():
        if k in file_header:
            continue
        else:
            file_header.append(k)

def get_data_for_retail_sweeps():
    print("Getting today's 'RetailScraper' sweeps... ")
    ###sweeps = sql.get_todays_completed_sweeps('RetailScraper')
    sweeps = [1020,1021]
    print("Sweeps Found -> " + str(len(sweeps)))
    alldata = []
    for sweep in sweeps:
        data = es.get_data_for_field("store_data", "store_data_coll", "sweepid", sweep)

        for d in data:
            alldata.append(d)
            build_file_header(d)

        print("Data found for sweep - "+str(sweep) + " --> " + str(len(data)))

    return alldata


def sweep_data_to_csv(scrape_type, field_list):
    folder = "C:\\temp\\"
    filename = datetime.date.today().strftime("Sweep Data %Y-%m-%d")
    global file_header
    data = []
    if scrape_type == 'retail':
        data = get_data_for_retail_sweeps()
    elif scrape_type == 'category':
        data = get_data_for_category_sweeps()
    else:
        return

    print("Total Rows found - "+str(len(data)))
    if len(data) == 0:
        print("Data not found!")
        return

    file = get_valid_filename(folder, filename, 1)
    print("Writing to file now -> " + file)
    if len(field_list) == 0:
        print("File headers not passed, using all headers")
        field_list = file_header

    write_data_file(data, file, field_list)


def get_valid_filename(folder, filename, counter):
    path = "{folder}{filename} - {counter}.{extension}".format(folder=folder, filename=filename, counter=counter,
                                                               extension="csv")
    if os.path.isfile(path):
        return get_valid_filename(folder, filename, counter+1)
    else:
        return path


def write_data_file(data, file, field_list):
    fields = ""
    for f in field_list:
        fields = fields + f + ","

    fl = codecs.open(file,"w","utf-8")
    fl.write(fields + "\n")
    counter = 0

    for d in data:
        try:
            counter += 1
            sb = ""
            for f in field_list:
                try:
                    r_val = d[f]
                    if r_val is None:
                        continue

                    val = str(r_val).strip().replace(",", "")
                    sb += val + ","
                except Exception as e:
                    sb += ","
            sb += "\n"
            fl.write(sb)
        except Exception as e:
            print(e)

    fl.close()


"""
field_list_category = ["sweepid", "pagenumber", "itemrank", "title", "price","condition", "productid", "uniqueident", "headervalue",
                       "categorypath", "quantity", "currency", "detail_link", "offerdate", "categoryurl", "createdon","baseprice","pricecut","price_withoutoffer",
                       "image_link", "sitename","site_product_id","product_id","document_id","storeid","uniqueident","site_product_id_hash","product_id_hash","document_id_hash", "imagelinkmessage", "description"]
"""
sweep_data_to_csv('category', [])