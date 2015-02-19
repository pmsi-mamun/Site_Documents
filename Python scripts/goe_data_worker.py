__author__ = 'mrashid'


from sql_wrapper.sqlWrapper import SQLWrapper
from pmsi_utility.utility import helper as um
import logging,uuid,json,datetime,arrow
from pylastic import ESWrapper

logger = logging.getLogger('DataValidationWorker')
um.setup_logger('c:\\temp\\', 'DataValidationWorker', logger)

sql = SQLWrapper('pmsisql02','ScrapingCosmos_Dev', False, 'Scraping_Bot_User', 'Scraping_Bot_User', logger)
#es = ESWrapper("84.40.63.146:9200",logger)
es = ESWrapper("Pmsi0087:9200",logger)
def get_geo_data_from_sql():
    data_rows = []
    json_rows = []

    country = 'AT'
    data_rows = sql.execute_stored_procedure("[dbo].[GetGeoPostcode]", country)
    for r in data_rows:
        r['geo_id'] = uuid.uuid4().__str__()
        #r['created_on'] = arrow.get(str(datetime.datetime.utcnow()), 'YYYY-MM-DD HH:mm:ss').format('YYYY-MM-DDTHH:mm:ss')
        r['status'] = 'available'
        r['Latitude'] = r['Lat']
        r['Longitude'] = r['Lng']
        del r['Lat']
        del r['Lng']
        json_rows.append(r)


    es.bulk_index(es.es_client,'default_work_list', 'geodata', json_rows, 'geo_id', bulk_size=1000)


get_geo_data_from_sql()