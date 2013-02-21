'''
Created on Feb 21, 2013

@author: stephane
'''
from investigator import Investigator
import feedparser
import datetime

class HourPopularity(Investigator):        
    def parse_popularity(self):
        data = feedparser.parse(self.url)
        app_list = []
        app_list_hourly = []
        
        for entry in data.entries:
            entry_id = int(entry.id.split('/id')[-1].split('id')[0].split('?')[0])
            search_entry = self.db.apps.find_one({'_id' : entry_id})
            # only search api if not in index or too old
            if search_entry is None or search_entry.get('index_update').date() < datetime.datetime.strptime(entry.get('updated', '')[:-6], "%Y-%m-%dT%H:%M:%S").date():
                app = self.search_api(entry_id)
                if app is None:
                    continue
            else:
                app = {'app_id' : search_entry.get('_id',''),
                       'name' : search_entry.get('name',''),
                       'icon' : search_entry.get('icon',''),
                       'feature_graphic' : search_entry.get('feature_graphic',''),
                       }
            
            app_on_list_hourly = { 'app_id' : app['app_id'] }
            app_list.append(app)
            app_list_hourly.append(app_on_list_hourly)
            
        list = {'_id' : self.list_name,
                'apps' : app_list,
                }
        popularity_lists_id = self.db.popularity_lists.save(list)
        hourly_list = {'_id' :  self.list_name+'_'+str(datetime.datetime.now().hour),
                       'apps' : app_list_hourly,
                       }
        hourly_lists_id = self.db.hourly_lists.save(hourly_list)