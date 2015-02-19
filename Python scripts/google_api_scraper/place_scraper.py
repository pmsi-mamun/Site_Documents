__author__ = 'mrashid'



__author__ = 'amit'

import logging
from sql_wrapper.sqlWrapper import SQLWrapper
from google_places_api import GPlacesAPI
from pmsi_utility.utility import helper as um
from pylastic import ESWrapper

logger = logging.getLogger('GeoCrawlerSimulator')
um.setup_logger('C:\\temp\\', 'GeoCrawlerSimulator', logger)

sql = SQLWrapper('pmsisql02','ScrapingCosmos_Dev', False, 'Scraping_Bot_User', 'Scraping_Bot_User', logger)
es = ESWrapper(['pmsi0146:9200'], logger)

GPlaces = GPlacesAPI(logger)

google_establishment_types = 'grocery_or_supermarket'
place_id_traker = []
data_rows = sql.execute_stored_procedure("[dbo].[GetGeoPostcode]", 'ES')

for r in data_rows:
    geo_point = {'lat' : r['Lat'], 'lng': r['Lng']}
    radius = '50000'
    stores = []
    store = {}
    try:
        lat_long = "{0},{1}".format(geo_point['lat'], geo_point['lng'])
        goog_response = GPlaces.get_radar_paged_filtered_results(google_establishment_types,lat_long,radius,'Aldi')

        for r in goog_response['results']:
            if r['place_id'] not in place_id_traker:
                place_id_traker.append(r['place_id'])
                store = GPlaces.get_place_details_placeid(r['place_id'])
                if ('Website' in store) and (store['Website'] != 'https://www.aldi.es/' ):
                    continue
                store['Latitude'] = store['Geometry']['location']['lat']
                store['Longitude'] = store['Geometry']['location']['lng']
            if (len(store) > 0):
                stores.append(store)

        if (len(stores)> 0):
            es.bulk_index(es.es_client,'aldi_es_geo_data_feb', 'ES', stores, 'UniqueIdent', bulk_size=1000)
    except Exception as e:
        logger.error(e)

    if goog_response is None:
        logger.info('No Google results found')
    else:
        logger.info('Google results found -> %s', str(len(goog_response['results'])))


