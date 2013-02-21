'''
Created on Feb 20, 2013

@author: stephane
'''
from pymongo import MongoClient
import logging
import time
import threading
import datetime
from AppleRssFeedReader.popularity import HourPopularity

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
hdlr = logging.FileHandler('appdj.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 

MONGO_CONNECTION_URL = 'mongodb://localhost/appdj'

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

LISTS = {'topfreeapplications' : ['free', 'mobile' ], 
         'toppaidapplications' : ['paid', 'mobile' ], 
         'topgrossingapplications' : ['gross', 'mobile' ],
         'topfreeipadapplications' : ['free', 'tablet' ],
         'toppaidipadapplications' : ['paid', 'tablet' ],
         'topgrossingipadapplications' : ['gross', 'tablet' ]}

class CountryThread(threading.Thread):
    def __init__(self, country_code, country, db):
        threading.Thread.__init__(self)
        self.country_code = country_code
        self.country = country
        self.db = db
        
    def run(self):
        country_start = time.time()
        for list, identifier in LISTS.items():
            logger.info("Started list: %s for counrty: %s" % (list, self.country))
            list_start = time.time()
            all_url = 'https://itunes.apple.com/%s/rss/%s/limit=400/xml' % (self.country_code, list)
            all_name = 'p_%s_%s_all_h_%s' % (identifier[1], self.country_code, identifier[0])
            a = HourPopularity(all_url, all_name, self.country_code, self.db) # all categories
            a.parse_popularity()
            # for each category
            for catname, catnr in CATEGORIES.items():
                cat_start = time.time()
                url = 'https://itunes.apple.com/%s/rss/topfreeapplications/limit=400/genre=%s/xml' % (self.country_code, catnr)
                name = 'p_%s_%s_%s_h_%s' % (identifier[1], self.country_code, catnr, identifier[0])
                a = HourPopularity(url, name, self.country_code, self.db)
                a.parse_popularity()
                logger.info("Finished category %s for list %s in country %s: in %s seconds" % (catnr, list, self.country_code, time.time()-cat_start))
            
            logger.info("Finished list %s for %s: in %s seconds" % (list, self.country_code, time.time()-list_start))
            
        logger.info("Finished country: %s in %s seconds" % (self.country, time.time()-country_start))
        
def main():
    logger.info("Started at: %s" % (datetime.datetime.now()))
    connection = MongoClient(MONGO_CONNECTION_URL)
    db = connection.appdj
    
    # top free mobile
    for code, country in COUNTRY_CODES.items():
        c = CountryThread(code, country, db)
        c.start()

if __name__ == '__main__':
    main()