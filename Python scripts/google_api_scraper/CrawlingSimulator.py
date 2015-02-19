__author__ = 'amit'

import logging
from google_places_api import GPlacesAPI
from pmsi_utility.utility import helper as um


logger = logging.getLogger('GeoCrawlerSimulator')
um.setup_logger('C:\\temp\\', 'GeoCrawlerSimulator', logger)
GPlaces = GPlacesAPI(logger)

#google_establishment_types = 'grocery_or_supermarket|food|store|establishment'

google_establishment_types = 'grocery_or_supermarket'

final = GPlaces.get_place_details_placeid('ChIJg0wYYPrCTw0R5VUc9_thQdI')

geo_point = {'lat' : '40.440117', 'lng': '-3.672934'}
radius = '47022'

lat_long = "{0},{1}".format(geo_point['lat'], geo_point['lng'])
goog_response = GPlaces.get_radar_paged_results(google_establishment_types,lat_long,radius)

for r in goog_response['results']:
    #if r['place_id'] == str('ChIJbfFsi3j7cQ0RxSFXxBVwFzw'):
        #print('found!')
    final = GPlaces.get_place_details_placeid(r['place_id'])

    if ('Website' in final) and ('https://www.aldi.es/' in final['Website']):
        print(final['Name'])

if goog_response is None:
    logger.info('No Google results found')
else:
    logger.info('Google results found -> %s', str(len(goog_response['results'])))


