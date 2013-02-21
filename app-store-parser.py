'''
Created on Feb 20, 2013

@author: stephane
'''
import feedparser
from pymongo import MongoClient
import simplejson as json
import requests
import datetime

CATEGORIES = {'Book' : 6018,
              'Business' : 6000,
              'Catalogs' : 6022,
              'Education' : 6017,
              'Entertainment' : 6016,
              'Finance' : 6015,
              'Food&Drink' : 6023,
              'Games' : 6014,
              'Health&Fitness' : 6013,
              'Lifestyle' : 6012,
              'Medical' : 6020,
              'Music' : 6011,
              'Navigation' : 6010,
              'News' : 6009,
              'Newsstand' : 6021,
              'Photo&Video' : 6008,
              'Productivity' : 6007,
              'Reference' : 6006,
              'Social Networking' : 6005,
              'Sports' : 6004,
              'Travel' : 6003,
              'Utilities' : 6002,
              'Weather' : 6001,
              }

COUNTRY_CODES = {'gw': 'Guinea-Bissau', 'gt': 'Guatemala', 'gr': 'Greece', 'gy': 'Guyana', 
                 'gd': 'Grenada', 'gb': 'United Kingdom', 'gm': 'Gambia', 'gh': 'Ghana', 
                 'lb': 'Lebanon', 'lc': 'St. Lucia', 'la': 'Lao People\xe2\x80\x99s Democratic Republic', 
                 'tw': 'Taiwan', 'tt': 'Trinidad and Tobago', 'tr': 'Turkey', 'lk': 'Sri Lanka', 
                 'lv': 'Latvia', 'lt': 'Lithuania', 'lu': 'Luxembourg', 'lr': 'Liberia', 'th': 'Thailand', 
                 'td': 'Chad', 'tc': 'Turks and Caicos', 'do': 'Dominican Republic', 'dm': 'Dominica', 
                 'dk': 'Denmark', 'de': 'Germany', 'ye': 'Yemen', 'dz': 'Algeria', 'qa': 'Qatar', 'tm': 'Turkmenistan', 
                 'ee': 'Estonia', 'eg': 'Egypt', 'za': 'South Africa', 'ec': 'Ecuador', 'us': 'United States', 
                 'zw': 'Zimbabwe', 'es': 'Spain', 'tj': 'Tajikistan', 'ru': 'Russia', 'ro': 'Romania', 
                 'be': 'Belgium', 'bf': 'Burkina Faso', 'bg': 'Bulgaria', 'bb': 'Barbados', 'bm': 'Bermuda', 
                 'bn': 'Brunei', 'bo': 'Bolivia', 'bh': 'Bahrain', 'bj': 'Benin', 'bt': 'Bhutan', 'jm': 'Jamaica', 
                 'jo': 'Jordan', 'br': 'Brazil', 'bs': 'Bahamas', 'tz': 'Tanzania', 'by': 'Belarus', 'bz': 'Belize', 
                 'om': 'Oman', 'ua': 'Ukraine', 'bw': 'Botswana', 'mz': 'Mozambique', 'ch': 'Switzerland', 'co': 'Colombia', 
                 'cn': 'China', 'cl': 'Chile', 'ca': 'Canada', 'cg': 'Republic Of Congo', 'cz': 'Czech Republic', 
                 'cy': 'Cyprus', 'cr': 'Costa Rica', 'cv': 'Cape Verde', 'tn': 'Tunisia', 'pw': 'Palau', 'pt': 'Portugal', 
                 'py': 'Paraguay', 'pa': 'Panama', 'pg': 'Papua New Guinea', 'pe': 'Peru', 'pk': 'Pakistan', 
                 'ph': 'Philippines', 'pl': 'Poland', 'hr': 'Croatia', 'it': 'Italy', 'hk': 'Hong Kong', 'hn': 'Honduras', 
                 'vn': 'Vietnam', 'jp': 'Japan', 'md': 'Republic Of Moldova', 'mg': 'Madagascar', 'uy': 'Uruguay', 
                 'uz': 'Uzbekistan', 'sa': 'Saudi Arabia', 'ml': 'Mali', 'mo': 'Macau', 'mn': 'Mongolia', 'mk': 'Macedonia', 
                 'mu': 'Mauritius', 'mt': 'Malta', 'mw': 'Malawi', 'ms': 'Montserrat', 'mr': 'Mauritania', 'ug': 'Uganda', 
                 'my': 'Malaysia', 'mx': 'Mexico', 'il': 'Israel', 'vc': 'St. Vincent and The Grenadines', 
                 'ae': 'United Arab Emirates', 've': 'Venezuela', 'ag': 'Antigua and Barbuda', 'vg': 'British Virgin Islands', 
                 'ai': 'Anguilla', 'is': 'Iceland', 'am': 'Armenia', 'al': 'Albania', 'ao': 'Angola', 'sv': 'El Salvador', 
                 'ar': 'Argentina', 'au': 'Australia', 'at': 'Austria', 'in': 'India', 'az': 'Azerbaijan', 'ie': 'Ireland', 
                 'id': 'Indonesia', 'ni': 'Nicaragua', 'nl': 'Netherlands', 'no': 'Norway', 'na': 'Namibia', 'ne': 'Niger', 
                 'ng': 'Nigeria', 'nz': 'New Zealand', 'np': 'Nepal', 'fr': 'France', 'sb': 'Solomon Islands', 'fi': 'Finland', 
                 'fj': 'Fiji', 'hu': 'Hungary', 'fm': 'Federated States Of Micronesia', 'sz': 'Swaziland', 'kg': 'Kyrgyzstan', 
                 'ke': 'Kenya', 'sr': 'Suriname', 'kh': 'Cambodia', 'kn': 'St. Kitts and Nevis', 'st': 'Sao Tome and Principe',
                 'sk': 'Slovakia', 'kr': 'Republic Of Korea', 'si': 'Slovenia', 'kw': 'Kuwait', 'sn': 'Senegal', 
                 'sl': 'Sierra Leone', 'sc': 'Seychelles', 'kz': 'Kazakstan', 'ky': 'Cayman Islands', 'sg': 'Singapore', 'se': 'Sweden'}


class AppleRssFeed(self):
    
    def __init__(self, url, list_name):
        self.url = url
        self.list_name = list_name
        
    def parse_popularity(self):
        connection = MongoClient('mongodb://appdj:appdjapple@mongodb1.alwaysdata.com/appdj_apple')
        db = connection.appdj_apple
        data = feedparser.parse(self.url)
        apps = db.apps
        apps_localized = db.apps_localized
        popularity_lists = db.popularity_lists
        hourly_lists = db.hourly_lists
        
        app_list = []
        
        for enrty in data.feed.entries:
            searchapi_lookup = 'https://itunes.apple.com/lookup?id='+entry.im_id
            response = requests.get(searchapi_lookup)
            search_entry = json.load(response).results[0]
            app = {'_id' : search_entry.trackId,
                   'bundle_id' : search_entry.bundleId,
                   'name' : search_entry.trackName,
                   'categories' : search_entry.genreIds,
                   'primary_category' : search_entry.primaryGenreId,
                   'developer' : search_entry.artistName,
                   'developer_id' : search_entry.artistId,
                   'icon' : search_entry.artworkUrl60,
                   'feature_graphic' : search_entry.artworkUrl512,
                   'rating_currver' : search_entry.averageUserRatingForCurrentVersion,
                   'num_rating_currver' : search_entry.userRatingCountForCurrentVersion,
                   'rating' : search_entry.averageUserRating,
                   'num_rating' : search_entry.userRatingCount,
                   'maturity' : search_entry.contentAdvisoryRating,
                   'release_date' : search_entry.releaseDate,
                   'version' : search_entry.version,
                   'screenshots' : search_entry.screenshotUrls,
                   'screenshots_ipad' : search_entry.ipadScreenshotUrls,
                   'supported_devices' : search_entry.supportedDevices,
                   }
            app_id = apps.save(app)
            app_localized = {'app_id' : search_entry.trackId,
                             'description' : search_entry.description,
                             'price' : search_entry.price,
                             'currency' : search_entry.currency,
                             'relase_notes' : search_entry.releaseNotes,
                             }
            app_loc_id = apps_localized.save(app_localized)
            app_on_list = {'app_id' : search_entry.trackId,
                           'name' : search_entry.trackName,
                           'developer' : search_entry.artistName,
                           'icon' : search_entry.artworkUrl60,
                           'feature_graphic' : search_entry.artworkUrl512,
                           'rating' : search_entry.averageUserRating,
                           'screenshots' : search_entry.screenshotUrls,
                           'screenshots_ipad' : search_entry.ipadScreenshotUrls,
                        }
            app_list.append(app_on_list)
            
        list = {'_id' : self.list_name,
                'apps' : app_list,
                }
        popularity_lists_id = popularity_lists.save(list)
        hourly_list = {'_id' :  self.list_name+'_'+datetime.datetime.now().hour,
                       'apps' : app_list,
                       }
        hourly_lists_id = hourly_lists.save()
        
        
def main():
    # top free mobile
    for code, country in COUNTRY_CODES.items():
        url = 'https://itunes.apple.com/%s/rss/topfreeapplications/limit=400/xml' % (code, catnr)
        name = 'p_mobile_%s_all_h_free' % (code)
        AppRssFeed(url, name) # all categories
        # for each category
        for catname, catnr in CATEGORIES.items():
            url = 'https://itunes.apple.com/%s/rss/topfreeapplications/limit=400/genre=%s/xml' % (code, catnr)
            name = 'p_mobile_%s_%s_h_free' % (code, catnr)
            AppRssFeed(url, name)

if __name__ == '__main__':
    pass