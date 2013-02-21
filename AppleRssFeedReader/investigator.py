'''
Created on Feb 21, 2013

@author: stephane
'''
import requests
import datetime
import logging

logger = logging.getLogger('logger')

class Investigator:
    def __init__(self, url, list_name, country, db):
        self.url = url
        self.list_name = list_name
        self.country = country
        self.db = db
        
    def search_api(self, app_id):
        searchapi_lookup = 'https://itunes.apple.com/lookup?id=%s&country=%s' % (app_id, self.country)
        response = requests.get(searchapi_lookup)
        try:
            search_entry = response.json()['results'][0]
        except:
            logger.error("Could not find app at: %s, Entry id: %s" % (searchapi_lookup, app_id))
            return None
        app = {'_id' : search_entry.get('trackId',''),
               'bundle_id' : search_entry.get('bundleId',''),
               'name' : search_entry.get('trackName',''),
               'categories' : search_entry.get('genreIds',''),
               'primary_category' : search_entry.get('primaryGenreId',''),
               'developer' : search_entry.get('artistName',''),
               'developer_id' : search_entry.get('artistId',''),
               'icon' : search_entry.get('artworkUrl60',''),
               'feature_graphic' : search_entry.get('artworkUrl512',''),
               'maturity' : search_entry.get('contentAdvisoryRating',''),
               'release_date' : datetime.datetime.strptime(search_entry.get('releaseDate',''), "%Y-%m-%dT%H:%M:%SZ"),
               'index_update' : datetime.datetime.now(),
               'version' : search_entry.get('version',''),
               'screenshots' : search_entry.get('screenshotUrls',''),
               'screenshots_ipad' : search_entry.get('ipadScreenshotUrls',''),
               'supported_devices' : search_entry.get('supportedDevices',''),
               }
        app_id = self.db.apps.save(app)
        app_localized = {'app_id' : search_entry.get('trackId',''),
                         'country' : self.country,
                         'description' : search_entry.get('description',''),
                         'price' : search_entry.get('price',''),
                         'currency' : search_entry.get('currency',''),
                         'release_notes' : search_entry.get('releaseNotes',''),
                         'rating_currver' : search_entry.get('averageUserRatingForCurrentVersion',''),
                         'num_rating_currver' : search_entry.get('userRatingCountForCurrentVersion',''),
                         'rating' : search_entry.get('averageUserRating',''),
                         'num_rating' : search_entry.get('userRatingCount',''),
                         }
        app_loc_id = self.db.apps_localized.save(app_localized)
        
        return {'app_id' : search_entry.get('trackId',''),
                'name' : search_entry.get('trackName',''),
                'icon' : search_entry.get('artworkUrl60',''),
                'feature_graphic' : search_entry.get('artworkUrl512',''),
                }
        
    